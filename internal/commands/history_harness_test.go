package commands

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/vicentereig/whatsapp-cli/internal/store"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// This file holds the test double for the `history extend` journey: a store
// whose oldest-message cursor only moves once a chunk has actually been
// ingested, a WhatsApp client that captures the event handler and lets a test
// script inject synthetic *events.HistorySync chunks, and fixture builders for
// those chunks. No network, no real store — see history_extend_test.go for the
// tests using it.

// storedRecord is one StoreMessage call, kept so tests can compare what the
// initial sync and the on-demand backfill persist.
type storedRecord struct {
	ID            string
	ChatJID       string
	Sender        string
	Content       string
	Timestamp     time.Time
	IsFromMe      bool
	MediaType     string
	Filename      string
	URL           string
	DirectPath    string
	MimeType      string
	MediaKey      []byte
	FileSHA256    []byte
	FileEncSHA256 []byte
	FileLength    uint64
}

// fakeHistoryStore is an in-memory MessageStore. GetOldestMessage is derived
// from the messages that have been stored, so the cursor advances only after a
// chunk was ingested — the property the request loop reasons about.
type fakeHistoryStore struct {
	mu sync.Mutex

	chats    []store.Chat
	messages map[string][]storedRecord

	listChatsCalls []store.ListChatsParams
	downloaded     []string
}

func newFakeHistoryStore() *fakeHistoryStore {
	return &fakeHistoryStore{messages: map[string][]storedRecord{}}
}

// seed registers a chat and its already-synced messages, i.e. the state `sync`
// would have left behind.
func (f *fakeHistoryStore) seed(chatJID string, records ...storedRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.chats = append(f.chats, store.Chat{JID: chatJID, Name: chatJID})
	for _, r := range records {
		f.putLocked(r)
	}
}

func (f *fakeHistoryStore) putLocked(r storedRecord) {
	existing := f.messages[r.ChatJID]
	for i, e := range existing {
		if e.ID == r.ID {
			existing[i] = r
			return
		}
	}
	f.messages[r.ChatJID] = append(existing, r)
}

// records returns everything stored for a chat, in insertion order.
func (f *fakeHistoryStore) records(chatJID string) []storedRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]storedRecord, len(f.messages[chatJID]))
	copy(out, f.messages[chatJID])
	return out
}

func (f *fakeHistoryStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url, directPath, mimeType string,
	mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putLocked(storedRecord{
		ID: id, ChatJID: chatJID, Sender: sender, Content: content, Timestamp: timestamp, IsFromMe: isFromMe,
		MediaType: mediaType, Filename: filename, URL: url, DirectPath: directPath, MimeType: mimeType,
		MediaKey: mediaKey, FileSHA256: fileSHA256, FileEncSHA256: fileEncSHA256, FileLength: fileLength,
	})
	return nil
}

func (f *fakeHistoryStore) GetOldestMessage(chatJID string) (store.OldestMessageInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var oldest *storedRecord
	for i, r := range f.messages[chatJID] {
		if oldest == nil || r.Timestamp.Before(oldest.Timestamp) {
			oldest = &f.messages[chatJID][i]
		}
	}
	if oldest == nil {
		return store.OldestMessageInfo{}, sql.ErrNoRows
	}
	return store.OldestMessageInfo{
		ID:        oldest.ID,
		Timestamp: oldest.Timestamp,
		IsFromMe:  oldest.IsFromMe,
		Sender:    oldest.Sender,
	}, nil
}

func (f *fakeHistoryStore) ListChats(params store.ListChatsParams) ([]store.Chat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listChatsCalls = append(f.listChatsCalls, params)

	start := params.Page * params.Limit
	if start >= len(f.chats) {
		return nil, nil
	}
	end := start + params.Limit
	if end > len(f.chats) {
		end = len(f.chats)
	}
	page := make([]store.Chat, end-start)
	copy(page, f.chats[start:end])
	return page, nil
}

func (f *fakeHistoryStore) GetMessageForDownload(id string, chatJID *string) (store.MessageDownloadInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for jid, records := range f.messages {
		if chatJID != nil && jid != *chatJID {
			continue
		}
		for _, r := range records {
			if r.ID != id {
				continue
			}
			return store.MessageDownloadInfo{
				ID: r.ID, ChatJID: r.ChatJID, MediaType: r.MediaType, Filename: r.Filename,
				DirectPath: r.DirectPath, MimeType: r.MimeType, MediaKey: r.MediaKey,
				FileSHA256: r.FileSHA256, FileEncSHA256: r.FileEncSHA256, FileLength: r.FileLength,
			}, nil
		}
	}
	return store.MessageDownloadInfo{}, sql.ErrNoRows
}

func (f *fakeHistoryStore) MarkMediaDownloaded(id, chatJID, localPath string, downloadedAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.downloaded = append(f.downloaded, id)
	return nil
}

func (f *fakeHistoryStore) StoreChat(jid, name string, lastMessageTime time.Time) error { return nil }

func (f *fakeHistoryStore) ListMessages(params store.ListMessagesParams) ([]store.Message, error) {
	return nil, nil
}

func (f *fakeHistoryStore) SearchContacts(query string) ([]store.Contact, error) { return nil, nil }

func (f *fakeHistoryStore) Close() error { return nil }

// historyRequest is one captured RequestMoreHistory call.
type historyRequest struct {
	ChatJID  string
	AnchorID string
	Anchor   time.Time
	IsFromMe bool
	Sender   string
	Count    int
	Round    int // 1-based per chat
	Sequence int // 1-based across the whole run
}

// historyHarness wires an App to the fake store and a fake WhatsApp client.
// respond is the test script: it runs on every RequestMoreHistory and decides
// which chunk (if any) the "phone" sends back.
type historyHarness struct {
	t      *testing.T
	app    *App
	store  *fakeHistoryStore
	client *MockWAClient

	respond func(h *historyHarness, req historyRequest)

	mu       sync.Mutex
	handler  func(interface{})
	requests []historyRequest
	perChat  map[string]int
	wg       sync.WaitGroup
}

func newHistoryHarness(t *testing.T, respond func(h *historyHarness, req historyRequest)) *historyHarness {
	t.Helper()

	h := &historyHarness{
		t:       t,
		store:   newFakeHistoryStore(),
		respond: respond,
		perChat: map[string]int{},
	}

	h.client = &MockWAClient{
		StartSyncFunc: func(ctx context.Context, eventHandler func(interface{})) error {
			h.mu.Lock()
			h.handler = eventHandler
			h.mu.Unlock()
			return nil
		},
		RequestMoreHistoryFunc: func(ctx context.Context, chatJID, anchorMsgID string, anchorTimestamp time.Time, anchorIsFromMe bool, anchorSender string, count int) error {
			h.mu.Lock()
			h.perChat[chatJID]++
			req := historyRequest{
				ChatJID: chatJID, AnchorID: anchorMsgID, Anchor: anchorTimestamp,
				IsFromMe: anchorIsFromMe, Sender: anchorSender, Count: count,
				Round: h.perChat[chatJID], Sequence: len(h.requests) + 1,
			}
			h.requests = append(h.requests, req)
			h.mu.Unlock()

			if h.respond != nil {
				h.respond(h, req)
			}
			return nil
		},
	}

	h.app = NewAppWithDeps(h.client, h.store, t.TempDir(), "test")
	t.Cleanup(h.wg.Wait)
	return h
}

// deliver feeds an event to the handler StartSync captured, the way whatsmeow
// would from its own goroutine.
func (h *historyHarness) deliver(evt interface{}) {
	h.mu.Lock()
	handler := h.handler
	h.mu.Unlock()
	if handler == nil {
		h.t.Error("event delivered before StartSync registered a handler")
		return
	}
	handler(evt)
}

// deliverAfter delivers an event from another goroutine once delay has passed,
// simulating a phone that takes its time to answer.
func (h *historyHarness) deliverAfter(delay time.Duration, evt interface{}) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		time.Sleep(delay)
		h.deliver(evt)
	}()
}

func (h *historyHarness) capturedRequests() []historyRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]historyRequest, len(h.requests))
	copy(out, h.requests)
	return out
}

// run executes HistoryExtend and fails the test if it does not return — a
// bounded run that blocks until interrupted is the bug this guards against.
func (h *historyHarness) run(ctx context.Context, opts HistoryExtendOptions) string {
	h.t.Helper()
	done := make(chan string, 1)
	go func() { done <- h.app.HistoryExtend(ctx, opts) }()

	select {
	case result := <-done:
		return result
	case <-time.After(15 * time.Second):
		h.t.Fatal("HistoryExtend did not return: bounded runs must print JSON without an interrupt")
		return ""
	}
}

// historyExtendData mirrors the JSON payload `history extend` prints.
type historyExtendData struct {
	Extended      bool `json:"extended"`
	MessagesCount int  `json:"messages_count"`
	RequestsSent  int  `json:"requests_sent"`
	ChatsTargeted int  `json:"chats_targeted"`
	ChatsSkipped  int  `json:"chats_skipped"`
	ChatsStable   int  `json:"chats_stable"`
	ChatsTimedOut int  `json:"chats_timed_out"`
	ChatsFailed   int  `json:"chats_failed"`
	Interrupted   bool `json:"interrupted"`
}

// --- synthetic HistorySync fixtures ---------------------------------------

type chunkOption func(*waHistorySync.Conversation)

// endOfHistory marks the conversation with the protocol's explicit
// "nothing older remains on the primary device" flag.
func endOfHistory() chunkOption {
	return func(c *waHistorySync.Conversation) {
		c.EndOfHistoryTransfer = proto.Bool(true)
		c.EndOfHistoryTransferType = waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY.Enum()
	}
}

// withAltJID makes the payload report the chat under a second identifier, the
// way a LID-keyed answer to a phone-JID request looks.
func withAltJID(pnJID string) chunkOption {
	return func(c *waHistorySync.Conversation) { c.PnJID = proto.String(pnJID) }
}

func historyChunk(syncType waHistorySync.HistorySync_HistorySyncType, chatJID string, msgs []*waHistorySync.HistorySyncMsg, opts ...chunkOption) *events.HistorySync {
	conv := &waHistorySync.Conversation{
		ID:       proto.String(chatJID),
		Name:     proto.String("Chat " + chatJID),
		Messages: msgs,
	}
	for _, opt := range opts {
		opt(conv)
	}
	return &events.HistorySync{
		Data: &waHistorySync.HistorySync{
			SyncType:      syncType.Enum(),
			Conversations: []*waHistorySync.Conversation{conv},
		},
	}
}

// onDemandChunk is what the phone sends back in response to RequestMoreHistory.
func onDemandChunk(chatJID string, msgs []*waHistorySync.HistorySyncMsg, opts ...chunkOption) *events.HistorySync {
	return historyChunk(waHistorySync.HistorySync_ON_DEMAND, chatJID, msgs, opts...)
}

func historyMsg(chatJID, id, sender string, ts time.Time, payload *waE2E.Message) *waHistorySync.HistorySyncMsg {
	return &waHistorySync.HistorySyncMsg{
		Message: &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{
				RemoteJID:   proto.String(chatJID),
				FromMe:      proto.Bool(false),
				ID:          proto.String(id),
				Participant: proto.String(sender),
			},
			MessageTimestamp: proto.Uint64(uint64(ts.Unix())),
			Message:          payload,
		},
	}
}

func textMsg(chatJID, id, sender, text string, ts time.Time) *waHistorySync.HistorySyncMsg {
	return historyMsg(chatJID, id, sender, ts, &waE2E.Message{Conversation: proto.String(text)})
}

// mediaFixture is the metadata an ON_DEMAND chunk must preserve — the fields
// the old copied-down handler dropped on the floor.
type mediaFixture struct {
	URL           string
	DirectPath    string
	MimeType      string
	MediaKey      []byte
	FileSHA256    []byte
	FileEncSHA256 []byte
	FileLength    uint64
	Caption       string
	Filename      string
}

func newMediaFixture(seed string) mediaFixture {
	return mediaFixture{
		URL:           "https://mmg.whatsapp.net/" + seed,
		DirectPath:    "/v/t62." + seed,
		MimeType:      "application/octet-stream",
		MediaKey:      []byte("media-key-" + seed),
		FileSHA256:    []byte("sha-" + seed),
		FileEncSHA256: []byte("enc-sha-" + seed),
		FileLength:    4242,
		Caption:       "caption " + seed,
		Filename:      seed + ".bin",
	}
}

func imageMsg(chatJID, id, sender string, ts time.Time, m mediaFixture) *waHistorySync.HistorySyncMsg {
	return historyMsg(chatJID, id, sender, ts, &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		URL: proto.String(m.URL), DirectPath: proto.String(m.DirectPath), Mimetype: proto.String(m.MimeType),
		MediaKey: m.MediaKey, FileSHA256: m.FileSHA256, FileEncSHA256: m.FileEncSHA256,
		FileLength: proto.Uint64(m.FileLength), Caption: proto.String(m.Caption),
	}})
}

func videoMsg(chatJID, id, sender string, ts time.Time, m mediaFixture) *waHistorySync.HistorySyncMsg {
	return historyMsg(chatJID, id, sender, ts, &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
		URL: proto.String(m.URL), DirectPath: proto.String(m.DirectPath), Mimetype: proto.String(m.MimeType),
		MediaKey: m.MediaKey, FileSHA256: m.FileSHA256, FileEncSHA256: m.FileEncSHA256,
		FileLength: proto.Uint64(m.FileLength), Caption: proto.String(m.Caption),
	}})
}

func audioMsg(chatJID, id, sender string, ts time.Time, m mediaFixture) *waHistorySync.HistorySyncMsg {
	return historyMsg(chatJID, id, sender, ts, &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
		URL: proto.String(m.URL), DirectPath: proto.String(m.DirectPath), Mimetype: proto.String(m.MimeType),
		MediaKey: m.MediaKey, FileSHA256: m.FileSHA256, FileEncSHA256: m.FileEncSHA256,
		FileLength: proto.Uint64(m.FileLength),
	}})
}

func documentMsg(chatJID, id, sender string, ts time.Time, m mediaFixture) *waHistorySync.HistorySyncMsg {
	return historyMsg(chatJID, id, sender, ts, &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
		URL: proto.String(m.URL), DirectPath: proto.String(m.DirectPath), Mimetype: proto.String(m.MimeType),
		MediaKey: m.MediaKey, FileSHA256: m.FileSHA256, FileEncSHA256: m.FileEncSHA256,
		FileLength: proto.Uint64(m.FileLength), Caption: proto.String(m.Caption), FileName: proto.String(m.Filename),
	}})
}

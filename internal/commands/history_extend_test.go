package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vicentereig/whatsapp-cli/internal/store"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
)

var baseTime = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func parseHistoryExtendResponse(t *testing.T, result string) historyExtendData {
	t.Helper()
	resp := parseResponse(t, result)
	require.True(t, resp.Success, "history extend should succeed: %s", result)

	var data historyExtendData
	require.NoError(t, json.Unmarshal(resp.Data, &data))
	return data
}

func anchorRecord(chatJID, id string, ts time.Time) storedRecord {
	return storedRecord{ID: id, ChatJID: chatJID, Sender: chatJID, Content: "anchor", Timestamp: ts}
}

// noopDownloader stands in for the real media download so the worker never
// touches the network or the filesystem beyond the temp store dir.
func noopDownloader(ctx context.Context, info store.MessageDownloadInfo, targetPath string) (int64, error) {
	return 0, nil
}

// TestHistoryExtend_BoundedRequestsReturnsJSON pins the contract scripts rely
// on: --requests N does its work, waits out the trailing chunks and returns
// JSON. The context here is never cancelled, so a run that waits for an
// interrupt fails the test instead of hanging forever.
func TestHistoryExtend_BoundedRequestsReturnsJSON(t *testing.T) {
	const chatJID = "1234567890@s.whatsapp.net"

	h := newHistoryHarness(t, func(h *historyHarness, req historyRequest) {
		// Each answer carries one message older than the anchor, so the cursor
		// walks backwards exactly like a real backfill.
		older := baseTime.Add(-time.Duration(req.Round) * time.Hour)
		h.deliver(onDemandChunk(chatJID, []*waHistorySync.HistorySyncMsg{
			textMsg(chatJID, fmt.Sprintf("older-%d", req.Round), chatJID, "older message", older),
		}))
	})
	h.store.seed(chatJID, anchorRecord(chatJID, "anchor-1", baseTime))

	result := h.run(context.Background(), HistoryExtendOptions{
		ChatJID:         chatJID,
		Count:           25,
		Requests:        2,
		ResponseTimeout: 2 * time.Second,
		IdleWait:        20 * time.Millisecond,
	})

	data := parseHistoryExtendResponse(t, result)
	require.True(t, data.Extended)
	require.False(t, data.Interrupted, "bounded run must not report an interrupt")
	require.Equal(t, 2, data.RequestsSent)
	require.Equal(t, 2, data.MessagesCount)
	require.Equal(t, 1, data.ChatsTargeted)
	require.Zero(t, data.ChatsStable)
	require.Zero(t, data.ChatsTimedOut)
	require.Zero(t, data.ChatsFailed)

	// Every request anchors on the oldest message currently in the store.
	requests := h.capturedRequests()
	require.Len(t, requests, 2)
	require.Equal(t, "anchor-1", requests[0].AnchorID)
	require.True(t, requests[0].Anchor.Equal(baseTime))
	require.Equal(t, chatJID, requests[0].Sender)
	require.Equal(t, 25, requests[0].Count)
	require.Equal(t, "older-1", requests[1].AnchorID, "second request should anchor on the newly ingested oldest message")
}

// TestHistoryExtend_UntilStableWaitsForResponse covers the false-stability bug:
// the decision must be based on an ingested response, not on a fixed sleep. The
// phone here answers slowly, and the second answer carries nothing new.
func TestHistoryExtend_UntilStableWaitsForResponse(t *testing.T) {
	const chatJID = "1234567890@s.whatsapp.net"

	h := newHistoryHarness(t, func(h *historyHarness, req historyRequest) {
		var msgs []*waHistorySync.HistorySyncMsg
		if req.Round == 1 {
			msgs = []*waHistorySync.HistorySyncMsg{
				textMsg(chatJID, "older-1", chatJID, "older message", baseTime.Add(-time.Hour)),
			}
		}
		// Slow phone: the answer lands well after any fixed grace period would
		// have elapsed, and from another goroutine like whatsmeow's.
		h.deliverAfter(100*time.Millisecond, onDemandChunk(chatJID, msgs))
	})
	h.store.seed(chatJID, anchorRecord(chatJID, "anchor-1", baseTime))

	result := h.run(context.Background(), HistoryExtendOptions{
		ChatJID:         chatJID,
		UntilStable:     true,
		MaxRounds:       5,
		ResponseTimeout: 3 * time.Second,
		IdleWait:        20 * time.Millisecond,
	})

	data := parseHistoryExtendResponse(t, result)
	require.Equal(t, 2, data.RequestsSent, "should stop after the answer that moved nothing")
	require.Equal(t, 1, data.ChatsStable)
	require.Zero(t, data.ChatsTimedOut, "a slow but real answer is not a timeout")
	require.Equal(t, 1, data.MessagesCount)
}

// TestHistoryExtend_TimeoutIsNotStable covers the other half of the same bug: a
// phone that never answers must be reported as a timeout, never as
// end-of-history.
func TestHistoryExtend_TimeoutIsNotStable(t *testing.T) {
	const chatJID = "1234567890@s.whatsapp.net"

	// No responder: the phone is offline / rate-limited / paused.
	h := newHistoryHarness(t, nil)
	h.store.seed(chatJID, anchorRecord(chatJID, "anchor-1", baseTime))

	result := h.run(context.Background(), HistoryExtendOptions{
		ChatJID:         chatJID,
		UntilStable:     true,
		MaxRounds:       5,
		ResponseTimeout: 60 * time.Millisecond,
		IdleWait:        20 * time.Millisecond,
	})

	data := parseHistoryExtendResponse(t, result)
	require.Equal(t, 1, data.RequestsSent, "should stop asking a silent chat")
	require.Equal(t, 1, data.ChatsTimedOut)
	require.Zero(t, data.ChatsStable, "silence must not be counted as end of history")
	require.Zero(t, data.MessagesCount)
	require.False(t, data.Interrupted)
}

// TestHistoryExtend_EndOfHistoryMarkerStopsChat: when the payload carries the
// protocol's explicit end-of-history flag we stop immediately, even though this
// chunk did move the cursor.
func TestHistoryExtend_EndOfHistoryMarkerStopsChat(t *testing.T) {
	const chatJID = "1234567890@s.whatsapp.net"

	h := newHistoryHarness(t, func(h *historyHarness, req historyRequest) {
		h.deliver(onDemandChunk(chatJID, []*waHistorySync.HistorySyncMsg{
			textMsg(chatJID, "older-1", chatJID, "the very first message", baseTime.Add(-time.Hour)),
		}, endOfHistory()))
	})
	h.store.seed(chatJID, anchorRecord(chatJID, "anchor-1", baseTime))

	result := h.run(context.Background(), HistoryExtendOptions{
		ChatJID:         chatJID,
		UntilStable:     true,
		MaxRounds:       5,
		ResponseTimeout: 2 * time.Second,
		IdleWait:        20 * time.Millisecond,
	})

	data := parseHistoryExtendResponse(t, result)
	require.Equal(t, 1, data.RequestsSent)
	require.Equal(t, 1, data.ChatsStable)
	require.Equal(t, 1, data.MessagesCount)
}

// TestHistoryExtend_MatchesResponseUnderAlternateJID: the phone may answer about
// a chat under its other identifier (LID vs phone JID). That still counts as an
// answer, not as silence.
func TestHistoryExtend_MatchesResponseUnderAlternateJID(t *testing.T) {
	const phoneJID = "1234567890@s.whatsapp.net"
	const lidJID = "9876543210@lid"

	h := newHistoryHarness(t, func(h *historyHarness, req historyRequest) {
		h.deliver(onDemandChunk(lidJID, nil, withAltJID(phoneJID)))
	})
	h.store.seed(phoneJID, anchorRecord(phoneJID, "anchor-1", baseTime))

	result := h.run(context.Background(), HistoryExtendOptions{
		ChatJID:         phoneJID,
		UntilStable:     true,
		MaxRounds:       3,
		ResponseTimeout: 200 * time.Millisecond,
		IdleWait:        20 * time.Millisecond,
	})

	data := parseHistoryExtendResponse(t, result)
	require.Zero(t, data.ChatsTimedOut, "an answer under the alternate JID should match the request")
	require.Equal(t, 1, data.ChatsStable)
}

// TestHistoryExtend_SkipsChatsWithoutAnchor: a chat with nothing stored locally
// has no cursor to request from, so it is skipped rather than requested.
func TestHistoryExtend_SkipsChatsWithoutAnchor(t *testing.T) {
	h := newHistoryHarness(t, nil)
	h.store.seed("empty@s.whatsapp.net")

	result := h.run(context.Background(), HistoryExtendOptions{
		All:             true,
		Requests:        1,
		ResponseTimeout: time.Second,
		IdleWait:        20 * time.Millisecond,
	})

	data := parseHistoryExtendResponse(t, result)
	require.Equal(t, 1, data.ChatsSkipped)
	require.Zero(t, data.RequestsSent)
	require.Empty(t, h.capturedRequests())
}

// TestHistoryExtend_AllPaginatesPastFirstPage: --all promises every chat in the
// local store, so it must page instead of stopping at the first batch.
func TestHistoryExtend_AllPaginatesPastFirstPage(t *testing.T) {
	const totalChats = 1200

	h := newHistoryHarness(t, func(h *historyHarness, req historyRequest) {
		// Answer with nothing new so every chat finishes in a single round.
		h.deliver(onDemandChunk(req.ChatJID, nil))
	})
	for i := 0; i < totalChats; i++ {
		jid := fmt.Sprintf("chat-%d@s.whatsapp.net", i)
		h.store.seed(jid, anchorRecord(jid, fmt.Sprintf("anchor-%d", i), baseTime))
	}

	result := h.run(context.Background(), HistoryExtendOptions{
		All:             true,
		Requests:        1,
		ResponseTimeout: 2 * time.Second,
		IdleWait:        20 * time.Millisecond,
	})

	data := parseHistoryExtendResponse(t, result)
	require.Equal(t, totalChats, data.ChatsTargeted, "every chat in the store should be targeted")
	require.Equal(t, totalChats, data.RequestsSent)

	requested := map[string]bool{}
	for _, req := range h.capturedRequests() {
		requested[req.ChatJID] = true
	}
	require.True(t, requested["chat-1000@s.whatsapp.net"], "chats past the old 1000-chat cap must be requested")
	require.True(t, requested[fmt.Sprintf("chat-%d@s.whatsapp.net", totalChats-1)], "the last chat must be requested")

	h.store.mu.Lock()
	calls := append([]store.ListChatsParams(nil), h.store.listChatsCalls...)
	h.store.mu.Unlock()

	require.Len(t, calls, 3, "%d chats at %d per page should take 3 calls", totalChats, historyChatPageSize)
	for i, c := range calls {
		require.Equal(t, historyChatPageSize, c.Limit)
		require.Equal(t, i, c.Page, "pages must advance")
	}
}

// TestHistoryExtend_ListenBlocksUntilInterrupted: the old block-forever
// behaviour is still available, but only when asked for explicitly.
func TestHistoryExtend_ListenBlocksUntilInterrupted(t *testing.T) {
	const chatJID = "1234567890@s.whatsapp.net"

	h := newHistoryHarness(t, func(h *historyHarness, req historyRequest) {
		h.deliver(onDemandChunk(chatJID, []*waHistorySync.HistorySyncMsg{
			textMsg(chatJID, "older-1", chatJID, "older message", baseTime.Add(-time.Hour)),
		}))
	})
	h.store.seed(chatJID, anchorRecord(chatJID, "anchor-1", baseTime))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Stands in for Ctrl+C once the bounded work is done.
	time.AfterFunc(150*time.Millisecond, cancel)

	result := h.run(ctx, HistoryExtendOptions{
		ChatJID:         chatJID,
		Requests:        1,
		Listen:          true,
		ResponseTimeout: 2 * time.Second,
		IdleWait:        20 * time.Millisecond,
	})

	data := parseHistoryExtendResponse(t, result)
	require.True(t, data.Interrupted, "--listen should keep running until interrupted")
	require.Equal(t, 1, data.RequestsSent)
	require.Equal(t, 1, data.MessagesCount)
}

// mediaChunkMessages is one message of every supported kind, used to compare
// what `sync` and `history extend` persist.
func mediaChunkMessages(chatJID string) []*waHistorySync.HistorySyncMsg {
	sender := "5511999999999@s.whatsapp.net"
	return []*waHistorySync.HistorySyncMsg{
		textMsg(chatJID, "msg-text", sender, "plain text", baseTime.Add(-5*time.Hour)),
		imageMsg(chatJID, "msg-image", sender, baseTime.Add(-4*time.Hour), newMediaFixture("image")),
		videoMsg(chatJID, "msg-video", sender, baseTime.Add(-3*time.Hour), newMediaFixture("video")),
		audioMsg(chatJID, "msg-audio", sender, baseTime.Add(-2*time.Hour), newMediaFixture("audio")),
		documentMsg(chatJID, "msg-document", sender, baseTime.Add(-time.Hour), newMediaFixture("document")),
	}
}

func sortedRecords(records []storedRecord, skipIDs ...string) []storedRecord {
	skip := map[string]bool{}
	for _, id := range skipIDs {
		skip[id] = true
	}
	var out []storedRecord
	for _, r := range records {
		if !skip[r.ID] {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// TestOnDemandChunkPersistsSameFieldsAsSync is the regression guard for the
// copied-down handler that stored on-demand media as blank text rows: both flows
// must persist the same records for the same payload.
func TestOnDemandChunkPersistsSameFieldsAsSync(t *testing.T) {
	const chatJID = "1234567890@s.whatsapp.net"

	// --- what `sync` stores for an initial-sync chunk
	syncStore := newFakeHistoryStore()
	syncCtx, cancelSync := context.WithCancel(context.Background())
	defer cancelSync()
	syncClient := &MockWAClient{
		StartSyncFunc: func(ctx context.Context, eventHandler func(interface{})) error {
			eventHandler(historyChunk(waHistorySync.HistorySync_INITIAL_BOOTSTRAP, chatJID, mediaChunkMessages(chatJID)))
			cancelSync() // Ctrl+C once the chunk has been ingested
			return nil
		},
	}
	syncApp := NewAppWithDeps(syncClient, syncStore, t.TempDir(), "test")
	syncApp.mediaDownloader = noopDownloader
	_ = syncApp.Sync(syncCtx)

	// --- what `history extend` stores for the same payload arriving on-demand
	h := newHistoryHarness(t, func(h *historyHarness, req historyRequest) {
		h.deliver(onDemandChunk(chatJID, mediaChunkMessages(chatJID)))
	})
	h.app.mediaDownloader = noopDownloader
	h.store.seed(chatJID, anchorRecord(chatJID, "anchor-1", baseTime))

	result := h.run(context.Background(), HistoryExtendOptions{
		ChatJID:         chatJID,
		Requests:        1,
		ResponseTimeout: 2 * time.Second,
		IdleWait:        20 * time.Millisecond,
	})
	parseHistoryExtendResponse(t, result)

	fromSync := sortedRecords(syncStore.records(chatJID))
	fromExtend := sortedRecords(h.store.records(chatJID), "anchor-1")

	require.Len(t, fromSync, 5, "sync should have stored the whole chunk")
	require.Equal(t, fromSync, fromExtend, "on-demand chunks must persist the same fields as the initial sync")

	// Spell out the metadata the old handler dropped, so a regression names
	// itself instead of just failing an equality check.
	byID := map[string]storedRecord{}
	for _, r := range fromExtend {
		byID[r.ID] = r
	}
	for id, wantType := range map[string]string{
		"msg-image": "image", "msg-video": "video", "msg-audio": "audio", "msg-document": "document",
	} {
		rec := byID[id]
		require.Equal(t, wantType, rec.MediaType, "%s media type", id)
		require.NotEmpty(t, rec.DirectPath, "%s direct path", id)
		require.NotEmpty(t, rec.URL, "%s url", id)
		require.NotEmpty(t, rec.MimeType, "%s mime type", id)
		require.NotEmpty(t, rec.MediaKey, "%s media key", id)
		require.NotEmpty(t, rec.FileSHA256, "%s file sha256", id)
		require.NotEmpty(t, rec.FileEncSHA256, "%s file enc sha256", id)
		require.NotZero(t, rec.FileLength, "%s file length", id)
	}
	require.Equal(t, "document.bin", byID["msg-document"].Filename, "document filename should survive")
}

// TestIngestHistorySyncEnqueuesMediaDownloads: storing the metadata is only half
// the job — the shared ingester must also queue the media for download, which is
// what makes on-demand media actually reachable offline.
func TestIngestHistorySyncEnqueuesMediaDownloads(t *testing.T) {
	const chatJID = "1234567890@s.whatsapp.net"

	st := newFakeHistoryStore()
	app := NewAppWithDeps(&MockWAClient{}, st, t.TempDir(), "test")

	var mu sync.Mutex
	seen := map[string]bool{}
	app.mediaDownloader = func(ctx context.Context, info store.MessageDownloadInfo, targetPath string) (int64, error) {
		mu.Lock()
		seen[info.ID] = true
		mu.Unlock()
		return 0, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker := newMediaDownloadWorker(app, 2)
	worker.Start(ctx)
	defer worker.Stop()

	app.ingestHistorySync(ctx, onDemandChunk(chatJID, mediaChunkMessages(chatJID)), worker)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return seen["msg-image"] && seen["msg-video"] && seen["msg-audio"] && seen["msg-document"]
	}, 5*time.Second, 10*time.Millisecond, "every media message should be queued for download")

	mu.Lock()
	defer mu.Unlock()
	require.False(t, seen["msg-text"], "text messages have nothing to download")
}

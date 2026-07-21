package commands

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vicentereig/whatsapp-cli/internal/client"
	"github.com/vicentereig/whatsapp-cli/internal/output"
	"github.com/vicentereig/whatsapp-cli/internal/store"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types/events"
)

// Defaults applied to the zero value of HistoryExtendOptions.
const (
	defaultHistoryCount           = 50
	defaultHistoryRequests        = 1
	defaultHistoryMaxRounds       = 20
	defaultHistoryResponseTimeout = 30 * time.Second
	defaultHistoryIdleWait        = 5 * time.Second

	// historyChatPageSize is how many chats --all reads per ListChats call while
	// paginating the whole local store.
	historyChatPageSize = 500
)

// HistoryExtendOptions configures a `history extend` run.
type HistoryExtendOptions struct {
	// ChatJID targets a single chat. Mutually exclusive with All.
	ChatJID string
	// All targets every chat in the local store (paginated, no cap).
	All bool
	// Count is how many messages to ask for per request.
	Count int
	// Requests is the number of consecutive requests per chat, each walking the
	// cursor further back. Ignored when UntilStable is set.
	Requests int
	// UntilStable keeps requesting per chat until the phone confirms there is
	// nothing older, capped by MaxRounds.
	UntilStable bool
	// MaxRounds caps the per-chat iteration count when UntilStable is set.
	MaxRounds int
	// Listen keeps the process alive after the bounded work is done, until the
	// context is cancelled (Ctrl+C). Without it the command exits with JSON as
	// soon as the requested work is finished and trailing chunks have landed.
	Listen bool
	// ResponseTimeout is how long to wait for the phone's ON_DEMAND chunk for a
	// given chat before giving up on that chat.
	ResponseTimeout time.Duration
	// IdleWait is how long a bounded run keeps listening for trailing chunks
	// after the last request before printing JSON.
	IdleWait time.Duration
}

func (o HistoryExtendOptions) withDefaults() HistoryExtendOptions {
	if o.Count <= 0 {
		o.Count = defaultHistoryCount
	}
	if o.Requests <= 0 {
		o.Requests = defaultHistoryRequests
	}
	if o.MaxRounds <= 0 {
		o.MaxRounds = defaultHistoryMaxRounds
	}
	if o.ResponseTimeout <= 0 {
		o.ResponseTimeout = defaultHistoryResponseTimeout
	}
	if o.IdleWait <= 0 {
		o.IdleWait = defaultHistoryIdleWait
	}
	return o
}

// historyExtendResponse is the signal the event handler raises once an ON_DEMAND
// chunk for a chat has been ingested. It is what lets the request loop tell
// "the phone answered and has nothing older" apart from "the phone never
// answered".
type historyExtendResponse struct {
	chatJID      string
	messages     int
	endOfHistory bool
}

// errHistoryResponseTimeout means no ON_DEMAND chunk arrived for the requested
// chat in time. It is NOT end-of-history: the phone may be offline, slow, or
// rate-limited, so it is reported separately.
var errHistoryResponseTimeout = errors.New("no history sync response")

// historyResponseBuffer is deliberately generous: the event handler must never
// block on a full channel, and dropping a signal degrades a chat to a reported
// timeout rather than a wrong "stable" verdict.
const historyResponseBuffer = 256

// HistoryExtend triggers on-demand backfill of older messages from the user's
// primary device. Wraps whatsmeow.Client.BuildHistorySyncRequest + a peer
// SendMessage. Responses arrive as *events.HistorySync chunks (SyncType
// ON_DEMAND) and are ingested by ingestHistorySync — the very same code path
// the initial sync uses, so media metadata and media downloads behave
// identically in both flows.
//
// Targeting:
//   - ChatJID set: extend just that chat
//   - All: iterate every chat in the local store, paginated
//
// Per chat the loop is: read the oldest local message, ask the phone for what
// precedes it, wait for the phone's answer, then re-read the cursor. A chat
// stops early when
//
//   - the payload says end-of-history for it, or
//   - a chunk was ingested and the cursor still did not move (nothing older), or
//   - no chunk arrived within ResponseTimeout — counted as a timeout, never as
//     end-of-history.
//
// Bounded runs (Requests / UntilStable+MaxRounds) return JSON and exit once the
// work is done and trailing chunks have stopped arriving. Listen restores the
// old behaviour of blocking until interrupted.
func (a *App) HistoryExtend(ctx context.Context, opts HistoryExtendOptions) string {
	opts = opts.withDefaults()

	// Decide target set BEFORE connecting so a typo fails fast.
	targets, err := a.historyExtendTargets(opts)
	if err != nil {
		return output.Error(err)
	}

	// Written by the event handler goroutine, read when the run finishes.
	var messageCount atomic.Int64
	responses := make(chan historyExtendResponse, historyResponseBuffer)

	worker := newMediaDownloadWorker(a, 4)
	worker.Start(ctx)
	a.mediaWorker = worker
	defer func() {
		worker.Stop()
		worker.PrintSummary()
		if a.mediaWorker == worker {
			a.mediaWorker = nil
		}
	}()

	eventHandler := func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			details := client.HandleMessage(v)
			chatName := a.client.ResolveChatName(ctx, details.ChatJID, v)
			if chatName == "" {
				chatName = details.ChatJID
			}
			mediaType, filename, url, directPath, mimeType := "", "", "", "", ""
			var mediaKey, fileSHA256, fileEncSHA256 []byte
			var fileLength uint64
			if details.Media != nil {
				mediaType, filename = details.Media.Type, details.Media.Filename
				url, directPath, mimeType = details.Media.URL, details.Media.DirectPath, details.Media.MimeType
				mediaKey, fileSHA256, fileEncSHA256 = details.Media.MediaKey, details.Media.FileSHA256, details.Media.FileEncSHA256
				fileLength = details.Media.FileLength
			}
			a.store.StoreChat(details.ChatJID, chatName, details.Timestamp)
			a.store.StoreMessage(
				details.ID, details.ChatJID, details.Sender, details.Content, details.Timestamp, details.IsFromMe,
				mediaType, filename, url, directPath, mimeType,
				mediaKey, fileSHA256, fileEncSHA256, fileLength,
			)
			if directPath != "" && len(mediaKey) > 0 {
				worker.Enqueue(mediaJob{messageID: details.ID, chatJID: details.ChatJID})
			}
			messageCount.Add(1)

		case *events.HistorySync:
			onDemand := v.Data.GetSyncType() == waHistorySync.HistorySync_ON_DEMAND
			fmt.Fprintf(os.Stderr, "\n📜 %s chunk: %d conversations\n", strings.ToLower(v.Data.GetSyncType().String()), len(v.Data.Conversations))

			stored := 0
			for _, res := range a.ingestHistorySync(ctx, v, worker) {
				stored += res.messages
				if !onDemand {
					// Only on-demand chunks answer our requests; an initial-sync
					// chunk landing mid-run must not be mistaken for one.
					continue
				}
				// Signal under every identifier the payload used for the chat, so a
				// request made under a phone JID still matches a LID-keyed answer
				// (and vice versa).
				for _, jid := range append([]string{res.chatJID}, res.altJIDs...) {
					select {
					case responses <- historyExtendResponse{chatJID: jid, messages: res.messages, endOfHistory: res.endOfHistory}:
					default: // buffer full: the request loop falls back to its timeout
					}
				}
			}
			fmt.Fprintf(os.Stderr, "💬 cumulative messages this run: %d\n", messageCount.Add(int64(stored)))

		case *events.Connected:
			fmt.Fprintln(os.Stderr, "✓ Connected to WhatsApp")

		case *events.Disconnected:
			fmt.Fprintln(os.Stderr, "⚠ Disconnected from WhatsApp")
		}
	}

	fmt.Fprintln(os.Stderr, "🚀 Starting on-demand history extend...")
	if err := a.client.StartSync(ctx, eventHandler); err != nil {
		return output.Error(err)
	}

	result := historyExtendResult{chatsTargeted: len(targets)}
	maxIter := opts.Requests
	if opts.UntilStable {
		maxIter = opts.MaxRounds
	}

chats:
	for _, jid := range targets {
		for r := 0; r < maxIter; r++ {
			oldest, err := a.store.GetOldestMessage(jid)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					// Nothing stored for this chat yet: there is no anchor to
					// request from. `sync` has to seed it first.
					if r == 0 {
						result.skipped++
					}
				} else {
					fmt.Fprintf(os.Stderr, "  %s: cursor error: %v\n", jid, err)
					result.failed++
				}
				break
			}

			// Drop signals left over from earlier rounds so a stale chunk cannot
			// satisfy the wait we are about to start.
			drainPending(responses)

			if err := a.client.RequestMoreHistory(ctx, jid, oldest.ID, oldest.Timestamp, oldest.IsFromMe, oldest.Sender, opts.Count); err != nil {
				fmt.Fprintf(os.Stderr, "  %s: request failed: %v\n", jid, err)
				result.failed++
				break
			}
			result.requestsSent++
			fmt.Fprintf(os.Stderr, "  → %s (anchor=%s, %d) request %d\n", jid, oldest.ID, opts.Count, r+1)

			// Wait for the phone's answer for THIS chat before drawing any
			// conclusion — a silent phone must not look like end-of-history.
			resp, err := waitForChatResponse(ctx, responses, jid, opts.ResponseTimeout)
			if err != nil {
				if errors.Is(err, errHistoryResponseTimeout) {
					fmt.Fprintf(os.Stderr, "  %s: no response within %s — giving up on this chat (not end of history)\n", jid, opts.ResponseTimeout)
					result.timedOut++
					break
				}
				result.interrupted = true
				break chats
			}

			if resp.endOfHistory {
				fmt.Fprintf(os.Stderr, "  %s: phone reports no older messages\n", jid)
				result.stable++
				break
			}

			next, err := a.store.GetOldestMessage(jid)
			if err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					fmt.Fprintf(os.Stderr, "  %s: cursor error: %v\n", jid, err)
					result.failed++
				}
				break
			}
			if next.ID == oldest.ID {
				// A chunk was ingested and the oldest cursor still did not move:
				// the phone has nothing older for this chat.
				fmt.Fprintf(os.Stderr, "  %s: stable after %d request(s)\n", jid, r+1)
				result.stable++
				break
			}
		}
	}

	switch {
	case result.interrupted:
		// Ctrl+C during the loop: report what we got instead of dropping it.
	case opts.Listen:
		fmt.Fprintf(os.Stderr, "\n📨 Sent %d on-demand requests across %d chat(s). Listening for chunks... (Press Ctrl+C to stop)\n", result.requestsSent, result.chatsTargeted)
		<-ctx.Done()
		result.interrupted = true
	case result.requestsSent > 0:
		fmt.Fprintf(os.Stderr, "\n📨 Sent %d on-demand requests across %d chat(s). Waiting %s for trailing chunks...\n", result.requestsSent, result.chatsTargeted, opts.IdleWait)
		result.interrupted = drainResponses(ctx, responses, opts.IdleWait)
	}

	result.messages = int(messageCount.Load())
	return result.render()
}

// historyExtendTargets resolves the chats a run should walk. With All it pages
// through the entire local store — the earlier implementation stopped at the
// first 1000 chats while the help text promised every chat.
func (a *App) historyExtendTargets(opts HistoryExtendOptions) ([]string, error) {
	if !opts.All {
		if strings.TrimSpace(opts.ChatJID) == "" {
			return nil, fmt.Errorf("history extend requires --chat JID or --all")
		}
		return []string{opts.ChatJID}, nil
	}

	var targets []string
	for page := 0; ; page++ {
		chats, err := a.store.ListChats(store.ListChatsParams{Limit: historyChatPageSize, Page: page})
		if err != nil {
			return nil, fmt.Errorf("listing chats: %w", err)
		}
		if len(chats) == 0 {
			break
		}
		for _, c := range chats {
			targets = append(targets, c.JID)
		}
		if len(chats) < historyChatPageSize {
			break
		}
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no chats in local store; run `sync` first")
	}
	return targets, nil
}

// waitForChatResponse blocks until an ON_DEMAND chunk covering chatJID has been
// ingested, the timeout expires, or ctx is cancelled. Signals for other chats
// are discarded: those messages are already persisted, the channel only exists
// to tell the request loop that the phone replied.
func waitForChatResponse(ctx context.Context, responses <-chan historyExtendResponse, chatJID string, timeout time.Duration) (historyExtendResponse, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		select {
		case <-ctx.Done():
			return historyExtendResponse{}, ctx.Err()
		case <-deadline.C:
			return historyExtendResponse{}, errHistoryResponseTimeout
		case resp := <-responses:
			if resp.chatJID == chatJID {
				return resp, nil
			}
		}
	}
}

// drainResponses waits out the tail of a bounded run: the phone often answers a
// single request with several chunks. Returns once nothing has arrived for idle,
// or true if the context was cancelled first.
func drainResponses(ctx context.Context, responses <-chan historyExtendResponse, idle time.Duration) bool {
	for {
		select {
		case <-ctx.Done():
			return true
		case <-responses:
		case <-time.After(idle):
			return false
		}
	}
}

// drainPending discards signals that are already queued, without blocking.
func drainPending(responses <-chan historyExtendResponse) {
	for {
		select {
		case <-responses:
		default:
			return
		}
	}
}

type historyExtendResult struct {
	messages      int
	requestsSent  int
	chatsTargeted int
	skipped       int
	stable        int
	timedOut      int
	failed        int
	interrupted   bool
}

func (r historyExtendResult) render() string {
	return output.Success(map[string]interface{}{
		"extended":        true,
		"messages_count":  r.messages,
		"requests_sent":   r.requestsSent,
		"chats_targeted":  r.chatsTargeted,
		"chats_skipped":   r.skipped,
		"chats_stable":    r.stable,
		"chats_timed_out": r.timedOut,
		"chats_failed":    r.failed,
		"interrupted":     r.interrupted,
	})
}

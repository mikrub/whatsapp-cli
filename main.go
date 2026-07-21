package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/vicentereig/whatsapp-cli/internal/client"
	"github.com/vicentereig/whatsapp-cli/internal/commands"
)

// parseFullHistoryFlags scans `auth` args for --full-history [--days N].
// Hand-rolled rather than a FlagSet because the auth command has no FlagSet and
// adding one would change how existing invocations parse.
func parseFullHistoryFlags(args []string) (bool, uint32) {
	const defaultDays = 3650 // ~10y; the server clamps to whatever it actually has
	full := false
	days := uint32(defaultDays)
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--full-history", "-full-history":
			full = true
		case "--days", "-days":
			if i+1 < len(args) {
				var parsed uint32
				if _, err := fmt.Sscanf(args[i+1], "%d", &parsed); err == nil && parsed > 0 {
					days = parsed
				}
				i++
			}
		}
	}
	return full, days
}

var (
	// version is overridden at build time via -ldflags "-X main.version=X.Y.Z"
	version = "1.3.3"
)

const (
	// defaultTimeout is the maximum duration for non-sync commands
	defaultTimeout = 5 * time.Minute
)

// optionalStr returns nil for empty strings, otherwise a pointer to the string.
// Used to convert flag values to optional parameters.
func optionalStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

const usage = `WhatsApp CLI - Command line interface for WhatsApp

Usage:
  whatsapp-cli <command> [options]

Commands:
  auth                              Authenticate with WhatsApp (scan QR code)
  sync                              Sync messages continuously (run until Ctrl+C)
  messages list [--chat JID]        List messages
  messages search --query TEXT      Search messages
  contacts search --query TEXT      Search contacts
  chats list                        List chats
  send --to RECIPIENT --message TEXT                     Send a text message
  send --to RECIPIENT --image PATH [--caption TEXT]      Send an image
  media download --message-id ID [--chat JID] [--output PATH]   Download media for a message
  history extend --chat JID [--count N] [--requests R | --until-stable [--max-rounds N]]
                 [--response-timeout D] [--listen]
                                    On-demand backfill of older messages for one chat.
  history extend --all   [--count N] [--requests R | --until-stable [--max-rounds N]]
                                    Same, iterating every chat in the local store.
                                    With --until-stable, each chat stops being asked once
                                    the phone answers and has nothing older left.

                                    Exits with a JSON summary once the requested work is
                                    done; --listen keeps it running until Ctrl+C instead.
                                    Chats the phone never answered within
                                    --response-timeout (default 30s) are reported as
                                    chats_timed_out, separately from chats_stable.

                                    Notes:
                                    - Requires the WhatsApp app open and online on the
                                      primary phone — the linked-device protocol relays
                                      history through the phone, not via WA servers.
                                    - Single-writer constraint on store/: do not run
                                      'sync' and 'history extend' concurrently against
                                      the same store directory; they will corrupt the
                                      session.
                                    - The phone's "Chat history: Paused" status under
                                      Linked Devices does NOT block on-demand requests
                                      despite what it suggests.
  version                           Print CLI version information

Global Options:
  --store DIR    Storage directory (default: ./store)

Examples:
  whatsapp-cli auth
  whatsapp-cli sync                    # Keep running to sync messages
  whatsapp-cli messages list --chat 1234567890@s.whatsapp.net --limit 20
  whatsapp-cli messages search --query "meeting"
  whatsapp-cli contacts search --query "John"
  whatsapp-cli send --to 1234567890 --message "Hello"
  whatsapp-cli send --to 1234567890@g.us --message "Hello group"
`

// extractGlobalFlags pulls --store from anywhere in the arg list,
// returning the store directory and remaining args.
func extractGlobalFlags(args []string) (string, []string) {
	storeDir := "./store"
	var remaining []string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--store" && i+1 < len(args):
			storeDir = args[i+1]
			i++ // skip value
		case strings.HasPrefix(args[i], "--store="):
			storeDir = strings.TrimPrefix(args[i], "--store=")
		default:
			remaining = append(remaining, args[i])
		}
	}
	return storeDir, remaining
}

// errorJSON renders the CLI's error envelope. The message is encoded rather
// than interpolated so quotes coming out of flag parsing errors can't break the
// JSON that wrapping scripts parse.
func errorJSON(msg string) string {
	encoded, err := json.Marshal(msg)
	if err != nil {
		encoded = []byte(`"invalid error message"`)
	}
	return fmt.Sprintf(`{"success":false,"data":null,"error":%s}`, encoded)
}

func exitJSON(msg string) {
	fmt.Fprintln(os.Stderr, errorJSON(msg))
	os.Exit(1)
}

// parseHistoryExtendArgs parses the flags of `history extend` (args must already
// have the command and subcommand stripped) and rejects the combinations the
// help text advertises as mutually exclusive. Returns an error instead of
// exiting so it can be tested.
func parseHistoryExtendArgs(args []string) (commands.HistoryExtendOptions, error) {
	fs := flag.NewFlagSet("history extend", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	chatJID := fs.String("chat", "", "chat JID to extend (mutually exclusive with --all)")
	all := fs.Bool("all", false, "extend every chat in the local store")
	count := fs.Int("count", 50, "messages to request per call (whatsmeow recommends 50)")
	requests := fs.Int("requests", 1, "consecutive requests per chat — each walks the cursor further back")
	untilStable := fs.Bool("until-stable", false, "keep requesting per chat until the phone has nothing older (mutually exclusive with --requests)")
	maxRounds := fs.Int("max-rounds", 20, "with --until-stable: safety cap on per-chat iterations")
	listen := fs.Bool("listen", false, "after the requested work, keep listening for chunks until interrupted instead of exiting")
	responseTimeout := fs.Duration("response-timeout", 30*time.Second, "how long to wait for the phone's answer for a chat before giving up on it")

	if err := fs.Parse(args); err != nil {
		return commands.HistoryExtendOptions{}, err
	}

	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	switch {
	case *chatJID == "" && !*all:
		return commands.HistoryExtendOptions{}, errors.New("history extend requires exactly one of --chat or --all")
	case *chatJID != "" && *all:
		return commands.HistoryExtendOptions{}, errors.New("--chat and --all are mutually exclusive")
	case set["requests"] && set["until-stable"]:
		return commands.HistoryExtendOptions{}, errors.New("--requests and --until-stable are mutually exclusive")
	case set["max-rounds"] && !*untilStable:
		return commands.HistoryExtendOptions{}, errors.New("--max-rounds only applies with --until-stable")
	case *count < 1:
		return commands.HistoryExtendOptions{}, errors.New("--count must be at least 1")
	case *requests < 1:
		return commands.HistoryExtendOptions{}, errors.New("--requests must be at least 1")
	case *maxRounds < 1:
		return commands.HistoryExtendOptions{}, errors.New("--max-rounds must be at least 1")
	case *responseTimeout <= 0:
		return commands.HistoryExtendOptions{}, errors.New("--response-timeout must be positive")
	}

	return commands.HistoryExtendOptions{
		ChatJID:         *chatJID,
		All:             *all,
		Count:           *count,
		Requests:        *requests,
		UntilStable:     *untilStable,
		MaxRounds:       *maxRounds,
		Listen:          *listen,
		ResponseTimeout: *responseTimeout,
	}, nil
}

func requireSubcommand(args []string, command string, valid []string) string {
	if len(args) < 2 {
		exitJSON(fmt.Sprintf("%s requires a subcommand: %s", command, strings.Join(valid, ", ")))
	}
	sub := args[1]
	for _, v := range valid {
		if sub == v {
			return sub
		}
	}
	exitJSON(fmt.Sprintf("unknown %s subcommand: %s (valid: %s)", command, sub, strings.Join(valid, ", ")))
	return "" // unreachable
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	// Extract --store from anywhere in args,
	// so "whatsapp-cli contacts search --store /tmp" works.
	storeDir, args := extractGlobalFlags(os.Args[1:])

	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	command := args[0]

	if command == "version" {
		fmt.Printf(`{"success":true,"data":{"version":"%s"},"error":null}
`, version)
		return
	}

	// Create app
	absStoreDir, err := filepath.Abs(storeDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, `{"success":false,"data":null,"error":"invalid store path: %v"}`+"\n", err)
		os.Exit(1)
	}

	// Must happen before the client is built, because the full-sync request rides
	// in the companion-registration payload sent while pairing. Only meaningful
	// for `auth`; on an already-linked device it is silently inert.
	if command == "auth" {
		fullHistory, days := parseFullHistoryFlags(args)
		if fullHistory {
			client.EnableFullHistorySync(days)
			fmt.Fprintf(os.Stderr, "ℹ️  Requesting FULL history sync (%d days) — this pairing will be slower and larger.\n", days)
		}
	}
	app, err := commands.NewApp(absStoreDir, version)
	if err != nil {
		fmt.Fprintf(os.Stderr, `{"success":false,"data":null,"error":"Failed to initialize: %v"}
`, err)
		os.Exit(1)
	}
	defer app.Close()

	// Use different timeout for sync command
	var ctx context.Context
	var cancel context.CancelFunc
	if command == "sync" || command == "history" {
		// For sync and history extend, use signal-based cancellation
		// (these are long-running, listening for incoming events).
		ctx, cancel = context.WithCancel(context.Background())
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigChan
			cancel()
		}()
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), defaultTimeout)
	}
	defer cancel()

	var result string

	switch command {
	case "auth":
		result = app.Auth(ctx)

	case "sync":
		result = app.Sync(ctx)

	case "messages":
		subcommand := requireSubcommand(args, "messages", []string{"list", "search"})
		messagesCmd := flag.NewFlagSet("messages", flag.ExitOnError)
		chatJID := messagesCmd.String("chat", "", "chat JID")
		query := messagesCmd.String("query", "", "search query")
		limit := messagesCmd.Int("limit", 20, "limit")
		page := messagesCmd.Int("page", 0, "page")
		// Parse from args[2:] to skip subcommand ("list"/"search") —
		// Go's flag parser stops at the first non-flag argument.
		if len(args) > 2 {
			messagesCmd.Parse(args[2:])
		}

		switch subcommand {
		case "search":
			if *query == "" {
				exitJSON("messages search requires --query")
			}
			result = app.ListMessages(nil, query, *limit, *page)
		case "list":
			result = app.ListMessages(optionalStr(*chatJID), nil, *limit, *page)
		}

	case "contacts":
		requireSubcommand(args, "contacts", []string{"search"})
		contactsCmd := flag.NewFlagSet("contacts", flag.ExitOnError)
		query := contactsCmd.String("query", "", "search query")
		// Parse from args[2:] to skip subcommand ("search") —
		// Go's flag parser stops at the first non-flag argument.
		if len(args) > 2 {
			contactsCmd.Parse(args[2:])
		}

		if *query == "" {
			exitJSON("contacts search requires --query")
		}
		result = app.SearchContacts(*query)

	case "chats":
		requireSubcommand(args, "chats", []string{"list"})
		chatsCmd := flag.NewFlagSet("chats", flag.ExitOnError)
		query := chatsCmd.String("query", "", "search query")
		limit := chatsCmd.Int("limit", 20, "limit")
		page := chatsCmd.Int("page", 0, "page")
		// Parse from args[2:] to skip subcommand ("list") —
		// Go's flag parser stops at the first non-flag argument.
		if len(args) > 2 {
			chatsCmd.Parse(args[2:])
		}

		result = app.ListChats(optionalStr(*query), *limit, *page)

	case "send":
		sendCmd := flag.NewFlagSet("send", flag.ExitOnError)
		to := sendCmd.String("to", "", "recipient")
		message := sendCmd.String("message", "", "message text")
		image := sendCmd.String("image", "", "image file path")
		caption := sendCmd.String("caption", "", "image caption")
		sendCmd.Parse(args[1:])

		if *to == "" {
			exitJSON(`--to is required`)
		}
		if *image != "" && *message != "" {
			exitJSON(`--message and --image are mutually exclusive`)
		}
		if *image != "" {
			result = app.SendImage(ctx, *to, *image, *caption)
		} else if *message != "" {
			result = app.SendMessage(ctx, *to, *message)
		} else {
			exitJSON(`--message or --image required`)
		}

	case "history":
		requireSubcommand(args, "history", []string{"extend"})
		opts, err := parseHistoryExtendArgs(args[2:])
		if err != nil {
			exitJSON(err.Error())
		}
		result = app.HistoryExtend(ctx, opts)

	case "media":
		requireSubcommand(args, "media", []string{"download"})
		downCmd := flag.NewFlagSet("media download", flag.ExitOnError)
		messageID := downCmd.String("message-id", "", "message identifier")
		chatJID := downCmd.String("chat", "", "chat JID (optional)")
		outputPath := downCmd.String("output", "", "output file or directory")
		downCmd.Parse(args[2:])

		if *messageID == "" {
			exitJSON("--message-id required")
		}
		result = app.DownloadMedia(ctx, *messageID, optionalStr(*chatJID), *outputPath)

	default:
		fmt.Fprintf(os.Stderr, `{"success":false,"data":null,"error":"Unknown command: %s"}
`, command)
		os.Exit(1)
	}

	fmt.Println(result)
}

package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vicentereig/whatsapp-cli/internal/commands"
)

// TestParseHistoryExtendArgs_Valid checks the flags behave as the help text
// advertises. Args are what main passes in: command and subcommand stripped.
func TestParseHistoryExtendArgs_Valid(t *testing.T) {
	const chatJID = "1234567890@s.whatsapp.net"

	tests := []struct {
		name string
		args []string
		want commands.HistoryExtendOptions
	}{
		{
			name: "single chat uses documented defaults",
			args: []string{"--chat", chatJID},
			want: commands.HistoryExtendOptions{
				ChatJID: chatJID, Count: 50, Requests: 1, MaxRounds: 20, ResponseTimeout: 30 * time.Second,
			},
		},
		{
			name: "all chats",
			args: []string{"--all"},
			want: commands.HistoryExtendOptions{
				All: true, Count: 50, Requests: 1, MaxRounds: 20, ResponseTimeout: 30 * time.Second,
			},
		},
		{
			name: "bounded requests and count",
			args: []string{"--chat", chatJID, "--count", "100", "--requests", "3"},
			want: commands.HistoryExtendOptions{
				ChatJID: chatJID, Count: 100, Requests: 3, MaxRounds: 20, ResponseTimeout: 30 * time.Second,
			},
		},
		{
			name: "until stable with max rounds",
			args: []string{"--chat", chatJID, "--until-stable", "--max-rounds", "5"},
			want: commands.HistoryExtendOptions{
				ChatJID: chatJID, Count: 50, Requests: 1, UntilStable: true, MaxRounds: 5, ResponseTimeout: 30 * time.Second,
			},
		},
		{
			name: "listen and response timeout",
			args: []string{"--all", "--listen", "--response-timeout", "45s"},
			want: commands.HistoryExtendOptions{
				All: true, Count: 50, Requests: 1, MaxRounds: 20, Listen: true, ResponseTimeout: 45 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseHistoryExtendArgs(tt.args)
			require.NoError(t, err)
			require.Equal(t, tt.want, opts)
		})
	}
}

// TestParseHistoryExtendArgs_Rejected checks the combinations the help text
// presents as mutually exclusive are actually refused instead of silently
// picking a winner.
func TestParseHistoryExtendArgs_Rejected(t *testing.T) {
	const chatJID = "1234567890@s.whatsapp.net"

	tests := []struct {
		name        string
		args        []string
		wantContain string
	}{
		{
			name:        "neither chat nor all",
			args:        []string{},
			wantContain: "exactly one of --chat or --all",
		},
		{
			name:        "both chat and all",
			args:        []string{"--chat", chatJID, "--all"},
			wantContain: "mutually exclusive",
		},
		{
			name:        "requests together with until-stable",
			args:        []string{"--chat", chatJID, "--requests", "3", "--until-stable"},
			wantContain: "--requests and --until-stable are mutually exclusive",
		},
		{
			name:        "max-rounds without until-stable",
			args:        []string{"--chat", chatJID, "--max-rounds", "5"},
			wantContain: "--max-rounds only applies with --until-stable",
		},
		{
			name:        "zero count",
			args:        []string{"--chat", chatJID, "--count", "0"},
			wantContain: "--count must be at least 1",
		},
		{
			name:        "zero requests",
			args:        []string{"--chat", chatJID, "--requests", "0"},
			wantContain: "--requests must be at least 1",
		},
		{
			name:        "zero max-rounds",
			args:        []string{"--chat", chatJID, "--until-stable", "--max-rounds", "0"},
			wantContain: "--max-rounds must be at least 1",
		},
		{
			name:        "zero response timeout",
			args:        []string{"--chat", chatJID, "--response-timeout", "0s"},
			wantContain: "--response-timeout must be positive",
		},
		{
			name:        "unknown flag",
			args:        []string{"--chat", chatJID, "--nope"},
			wantContain: "not defined",
		},
		{
			name:        "malformed duration",
			args:        []string{"--chat", chatJID, "--response-timeout", "soon"},
			wantContain: "invalid value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseHistoryExtendArgs(tt.args)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantContain)
		})
	}
}

// TestErrorJSONStaysParseable: flag parsing errors quote the offending value, so
// the error envelope must encode the message instead of interpolating it.
func TestErrorJSONStaysParseable(t *testing.T) {
	_, err := parseHistoryExtendArgs([]string{"--chat", "x", "--response-timeout", "soon"})
	require.Error(t, err)
	require.Contains(t, err.Error(), `"`, "this error is expected to contain quotes")

	var envelope struct {
		Success bool    `json:"success"`
		Error   *string `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(errorJSON(err.Error())), &envelope))
	require.False(t, envelope.Success)
	require.NotNil(t, envelope.Error)
	require.Equal(t, err.Error(), *envelope.Error)
}

package daemon

import (
	"strconv"
	"strings"
	"testing"

	"github.com/sdougbrown/whinny/internal/wake"
	"github.com/sdougbrown/whinny/internal/watch"
)

func TestBuildEnvelope(t *testing.T) {
	tests := []struct {
		name     string
		decision wake.Decision
		msg      wake.Msg
		want     string
	}{
		{
			name:     "mention",
			decision: wake.Decision{Wake: true, Kind: wake.KindMention, Reason: `mentioned "squire"`},
			msg:      wake.Msg{MessageID: "m1", RoomID: "1793", From: wake.From{UserID: "doug", Name: "Doug"}, Text: "squire can you look at the roster"},
			want: `[wake kind=mention room=1793 from=doug message_id=m1]
squire can you look at the roster`,
		},
		{
			name:     "reply",
			decision: wake.Decision{Wake: true, Kind: wake.KindReply, Reason: "reply to my message"},
			msg:      wake.Msg{MessageID: "m2", RoomID: "r1", From: wake.From{UserID: "u2"}, Text: "actually no"},
			want: `[wake kind=reply room=r1 from=u2 message_id=m2]
actually no`,
		},
		{
			name:     "ambient",
			decision: wake.Decision{Wake: true, Kind: wake.KindAmbient, Reason: "ambient"},
			msg:      wake.Msg{MessageID: "m3", RoomID: "r1", From: wake.From{UserID: "u3"}, Text: "anyone around?"},
			want: `[wake kind=ambient room=r1 from=u3 message_id=m3]
anyone around?`,
		},
		{
			name: "watch with note",
			decision: wake.Decision{
				Wake:   true,
				Kind:   wake.KindWatch,
				Reason: "umpire posted a review",
				Watches: []wake.WatchHit{
					{ID: "w3", Owner: "resident", Note: `umpire posted a review`, Once: true},
				},
			},
			msg: wake.Msg{MessageID: "m4", RoomID: "r1", From: wake.From{UserID: "umpire-bot"}, Text: "PR 42 · review posted"},
			want: `[wake kind=watch id=w3 note="umpire posted a review" room=r1 from=umpire-bot message_id=m4]
PR 42 · review posted`,
		},
		{
			name: "note with quotes and spaces escapes",
			decision: wake.Decision{
				Wake:    true,
				Kind:    wake.KindWatch,
				Watches: []wake.WatchHit{{ID: "w4", Note: `say "hi" \ now`, Once: false}},
			},
			msg: wake.Msg{MessageID: "m5", RoomID: "r 2", From: wake.From{UserID: "u5"}},
			want: `[wake kind=watch id=w4 note="say \"hi\" \\ now" room="r 2" from=u5 message_id=m5]
`,
		},
		{
			name: "note with newline and control byte escapes",
			decision: wake.Decision{
				Wake:    true,
				Kind:    wake.KindWatch,
				Watches: []wake.WatchHit{{ID: "w6", Note: "line1\nline2\x01end", Once: true}},
			},
			msg: wake.Msg{MessageID: "m6", RoomID: "r1", From: wake.From{UserID: "u6"}},
			want: `[wake kind=watch id=w6 note="line1\nline2\x01end" room=r1 from=u6 message_id=m6]
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := BuildEnvelope(tt.decision, tt.msg)
			if env.Text != tt.want {
				t.Errorf("Text = %q, want %q", env.Text, tt.want)
			}
			if env.From != tt.msg.From.UserID {
				t.Errorf("From = %q, want %q", env.From, tt.msg.From.UserID)
			}
		})
	}
}

func TestExpiryEnvelope(t *testing.T) {
	w := watch.Watch{ID: "w3", Note: "umpire posted a review", Owner: "resident"}
	env := ExpiryEnvelope(w)
	want := `[wake kind=watch-expired id=w3 note="umpire posted a review"]
no match within ttl`
	if env.Text != want {
		t.Errorf("Text = %q, want %q", env.Text, want)
	}
}

// TestQuoteHeaderControlCharsKeepsOneLine: a note carrying a newline and
// a control byte must stay one parseable header line and round-trip
// through the escapes.
func TestQuoteHeaderControlCharsKeepsOneLine(t *testing.T) {
	note := "line1\nline2\x01end"
	env := BuildEnvelope(wake.Decision{
		Wake:    true,
		Kind:    wake.KindWatch,
		Watches: []wake.WatchHit{{ID: "w6", Note: note, Once: true}},
	}, wake.Msg{MessageID: "m6", RoomID: "r1", From: wake.From{UserID: "u6"}})

	lines := strings.SplitN(env.Text, "\n", 2)
	header := lines[0]
	if !strings.HasPrefix(header, "[wake") || !strings.HasSuffix(header, "]") {
		t.Fatalf("header = %q, want one line starting [wake and ending ]", header)
	}
	// The escaped note field round-trips to the original note.
	i := strings.Index(header, "note=")
	if i < 0 {
		t.Fatalf("header = %q, want a note field", header)
	}
	field := header[i+len("note="):]
	if !strings.HasPrefix(field, `"`) {
		t.Fatalf("note field = %q, want quoted", field)
	}
	end := strings.Index(field[1:], `"`) + 1
	if end < 0 {
		t.Fatalf("note field = %q, want a closing quote", field)
	}
	if got := unescapeHeader(field[1:end]); got != note {
		t.Errorf("round-trip = %q, want %q", got, note)
	}
}

// unescapeHeader inverts quoteHeader's escapes for the round-trip check.
func unescapeHeader(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b.WriteByte(s[i])
			continue
		}
		switch s[i+1] {
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case '"':
			b.WriteByte('"')
		case '\\':
			b.WriteByte('\\')
		case 'x':
			if i+3 < len(s) {
				v, err := strconv.ParseUint(s[i+2:i+4], 16, 8)
				if err == nil {
					b.WriteByte(byte(v))
					i += 3
					continue
				}
			}
			b.WriteString(s[i:])
			return b.String()
		default:
			b.WriteByte(s[i+1])
		}
		i++
	}
	return b.String()
}

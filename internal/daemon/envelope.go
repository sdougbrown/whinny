package daemon

import (
	"fmt"
	"strings"

	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/wake"
	"github.com/sdougbrown/whinny/internal/watch"
)

// Envelope is one wake delivery. Text is the envelope document the
// deliverer hands to the target (header line plus message text). From is
// the triggering message's author (the feed backend's "from" field).
// ImageURL is the first image embed URL on the triggering message, ""
// when the message carries none; only the feed backend fetches it.
type Envelope struct {
	Text     string
	From     string
	ImageURL string
}

// BuildEnvelope builds the wake envelope for a fired decision, per the design
// doc's daemon↔harness contract:
//
//	[wake kind=mention room=<room_id> from=<user_id> message_id=<id>]
//	<message text>
//
// A watch wake carries the primary hit's id and note on the header line;
// watch-expired envelopes are built by ExpiryEnvelope instead.
func BuildEnvelope(d wake.Decision, msg wake.Msg) Envelope {
	var b strings.Builder
	b.WriteString("[wake kind=")
	b.WriteString(string(d.Kind))
	if len(d.Watches) > 0 {
		hit := d.Watches[0]
		b.WriteString(" id=")
		b.WriteString(quoteHeader(hit.ID))
		b.WriteString(" note=")
		b.WriteString(quoteHeader(hit.Note))
	}
	b.WriteString(" room=")
	b.WriteString(quoteHeader(msg.RoomID))
	b.WriteString(" from=")
	b.WriteString(quoteHeader(msg.From.UserID))
	b.WriteString(" message_id=")
	b.WriteString(quoteHeader(msg.MessageID))
	b.WriteString("]\n")
	b.WriteString(msg.Text)
	return Envelope{Text: b.String(), From: msg.From.UserID}
}

// ReactionEnvelope builds the wake envelope for a reaction-set push on
// a message: the header is the standard wake line with kind=reaction
// and the reacting author, and the body is the reaction set's emoji —
// one author per line when several sets arrived.
func ReactionEnvelope(rec apronws.ReactionsRecord) Envelope {
	var b strings.Builder
	b.WriteString("[wake kind=reaction room=")
	b.WriteString(quoteHeader(rec.RoomID))
	b.WriteString(" from=")
	b.WriteString(quoteHeader(rec.Reactions[0].From.UserID))
	b.WriteString(" message_id=")
	b.WriteString(quoteHeader(rec.MessageID))
	b.WriteString("]\n")
	for i, set := range rec.Reactions {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(strings.Join(set.Emojis, " "))
		if set.From.UserID != "" {
			b.WriteString(" from ")
			b.WriteString(set.From.UserID)
		}
	}
	from := ""
	if len(rec.Reactions) > 0 {
		from = rec.Reactions[0].From.UserID
	}
	return Envelope{Text: b.String(), From: from}
}

// ExpiryEnvelope builds the wake envelope for a watch that expired
// without matching: the header carries the expired watch's id and note,
// and the text is fixed.
func ExpiryEnvelope(w watch.Watch) Envelope {
	var b strings.Builder
	b.WriteString("[wake kind=watch-expired id=")
	b.WriteString(quoteHeader(w.ID))
	b.WriteString(" note=")
	b.WriteString(quoteHeader(w.Note))
	b.WriteString("]\nno match within ttl")
	return Envelope{Text: b.String(), From: w.Owner}
}

// quoteHeader renders one header value. Values made only of header-safe
// characters go out bare; anything else (spaces, quotes, empties) is
// double-quoted with escapes, so a note like `umpire posted "v2"` stays
// one parseable field. Control characters are escaped (\n, \r, \t, and
// any other byte < 0x20 or 0x7f as \xHH) so a note with a newline can
// never split the single-line wake header.
func quoteHeader(s string) string {
	if s != "" && strings.IndexFunc(s, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
			r == '_' || r == '-' || r == '.' || r == '@' || r == ':' || r == '/')
	}) < 0 {
		return s
	}
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 || r == 0x7f {
				b.WriteString(fmt.Sprintf(`\x%02x`, r))
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

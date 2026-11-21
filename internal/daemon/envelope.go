package daemon

import (
	"strings"

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
// one parseable field.
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
		case '"', '\\':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('"')
	return b.String()
}

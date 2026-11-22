package slack

import (
	"encoding/json"
	"fmt"
	"regexp"
)

// Msg is the daemon-facing message shape the wake policy and cursor
// dedupe consume.
type Msg struct {
	// RoomID is the Slack channel id.
	RoomID string
	// MessageID is the message ts; for deletes it is the tombstone ts
	// and DeletedTS carries the removed message's ts.
	MessageID string
	// FromUserID is the human sender; BotID is set for bot and app
	// messages, which carry no user.
	FromUserID string
	BotID      string
	Text       string
	// ReplyTo is the thread parent ts.
	ReplyTo string
	// Subtype is Slack's message subtype ("" for plain messages).
	Subtype string
	// Mentions is the <@USERID> user ids parsed from Text (nil when
	// the text carries no mention token).
	Mentions []string
	// Edited marks an edit: a message_changed wrapper or a message
	// carrying an edited marker.
	Edited bool
	// DeletedTS is the deleted message's ts on a message_deleted event.
	DeletedTS string
}

// Message aliases Msg: the wire concept and the daemon-facing shape are
// the same struct.
type Message = Msg

// wireMessage is a Slack message event as delivered by Socket Mode or
// conversations.history.
type wireMessage struct {
	Type     string `json:"type"`
	Channel  string `json:"channel"`
	User     string `json:"user"`
	BotID    string `json:"bot_id"`
	Text     string `json:"text"`
	TS       string `json:"ts"`
	ThreadTS string `json:"thread_ts"`
	Subtype  string `json:"subtype"`
	// Edited is present (non-null) on messages edited in place, e.g. in
	// history responses.
	Edited json.RawMessage `json:"edited"`
	// Message is the inner message of a message_changed wrapper;
	// DeletedTS is set on a message_deleted event.
	Message   *wireMessage `json:"message"`
	DeletedTS string       `json:"deleted_ts"`
}

// DecodeMessage maps a Slack message event payload to the daemon-facing
// Msg shape. Edits arrive as message_changed wrappers and unwrap to the
// inner message with Edited set; deletes arrive as message_deleted and
// yield DeletedTS. Mentions are parsed from the text into Msg.Mentions.
func DecodeMessage(payload json.RawMessage) (Msg, error) {
	var w wireMessage
	if err := json.Unmarshal(payload, &w); err != nil {
		return Msg{}, err
	}
	switch w.Subtype {
	case "message_changed":
		if w.Message == nil {
			return Msg{}, fmt.Errorf("message_changed without inner message")
		}
		m := decodeWire(*w.Message)
		m.RoomID = w.Channel
		m.Edited = true
		return m, nil
	case "message_deleted":
		return Msg{
			RoomID:    w.Channel,
			MessageID: w.TS,
			Subtype:   w.Subtype,
			DeletedTS: w.DeletedTS,
		}, nil
	default:
		m := decodeWire(w)
		if len(w.Edited) > 0 && string(w.Edited) != "null" {
			m.Edited = true
		}
		return m, nil
	}
}

// decodeWire maps one wire message to Msg. Slack sets thread_ts == ts
// on thread-root (top-level) messages, so a non-empty thread_ts that
// equals the message's own ts is not a reply: only genuine thread
// replies carry a different thread_ts. Mentions are parsed from the
// text here, so every decoded message (live events and history) carries
// them.
func decodeWire(w wireMessage) Msg {
	replyTo := w.ThreadTS
	if replyTo == w.TS {
		replyTo = ""
	}
	return Msg{
		RoomID:     w.Channel,
		MessageID:  w.TS,
		FromUserID: w.User,
		BotID:      w.BotID,
		Text:       w.Text,
		ReplyTo:    replyTo,
		Subtype:    w.Subtype,
		Mentions:   ParseMentions(w.Text),
	}
}

// mentionRe matches a Slack user mention token, <@USERID>.
var mentionRe = regexp.MustCompile(`<@([A-Z0-9]+)>`)

// ParseMentions extracts every <@USERID> token from text, in order.
func ParseMentions(text string) []string {
	var out []string
	for _, m := range mentionRe.FindAllStringSubmatch(text, -1) {
		out = append(out, m[1])
	}
	return out
}

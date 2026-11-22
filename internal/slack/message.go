package slack

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
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
	// ImageURL is the first image attachment's fetch URL (a file's
	// url_private, else an attachment's image_url); "" when the message
	// carries no image.
	ImageURL string
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
	// Files and Attachments carry the message's image embeds (the
	// fetch URLs need the bot token's bearer auth).
	Files       []wireFile       `json:"files"`
	Attachments []wireAttachment `json:"attachments"`
}

// wireFile is a file attachment on a message event; url_private is the
// auth-gated fetch URL (files.slack.com).
type wireFile struct {
	MimeType   string `json:"mimetype"`
	URLPrivate string `json:"url_private"`
}

// wireAttachment is a block-kit attachment; image_url is the thumbnail
// URL on image attachments.
type wireAttachment struct {
	ImageURL string `json:"image_url"`
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
		ImageURL:   imageURL(w.Files, w.Attachments),
	}
}

// imageURL picks the first image attachment's fetch URL: a file with an
// image/ mimetype (its url_private), else an attachment with an
// image_url hosted on a slack.com host. The host restriction mirrors
// the daemon's slack fetcher allow-list (slack.HostAllowed): selecting
// an off-host URL would advertise an embed the fetcher refuses.
func imageURL(files []wireFile, attachments []wireAttachment) string {
	for _, f := range files {
		if strings.HasPrefix(f.MimeType, "image/") && f.URLPrivate != "" {
			return f.URLPrivate
		}
	}
	for _, a := range attachments {
		if a.ImageURL != "" && hostAllowed(a.ImageURL) {
			return a.ImageURL
		}
	}
	return ""
}

// hostAllowed reports whether rawURL's host (with optional port) is a
// slack.com host — the same allow-list the daemon's image fetcher
// enforces, shared so selection and fetch cannot disagree.
func hostAllowed(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return HostAllowed(u.Host)
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

// AuthorID attributes a message to a stable identity string: the human
// sender's user id when present, the bot's own user id when the message
// came from the resolving bot itself, or "bot:<bot id>" for other bots
// — so wake evaluation and history output agree on one identity per
// sender. Callers that know their own identity pass it; empty
// selfUserID/selfBotID skip the self-match.
func AuthorID(fromUserID, botID, selfUserID, selfBotID string) string {
	if fromUserID != "" {
		return fromUserID
	}
	if botID == "" {
		return ""
	}
	if botID == selfBotID && selfUserID != "" {
		return selfUserID
	}
	return "bot:" + botID
}

// HostAllowed reports whether host (with optional port) is a slack.com
// host: exactly slack.com or any subdomain. It is the allow-list both
// the image selection and the daemon's bearer-authenticated fetcher
// enforce.
func HostAllowed(host string) bool {
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	host = strings.ToLower(host)
	return host == "slack.com" || strings.HasSuffix(host, ".slack.com")
}

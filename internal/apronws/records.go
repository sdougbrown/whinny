package apronws

import (
	"encoding/json"
	"time"
)

// Kind discriminates the payload carried by a Record.
type Kind int

const (
	// KindMessage is an inbound chat message, or its delete tombstone.
	KindMessage Kind = iota
	// KindRoomUpdate is a room membership or metadata change.
	KindRoomUpdate
	// KindActivity is an activity push with an open-ended payload.
	KindActivity
)

// Record is a decoded inbound push. Exactly one of the payload fields is
// non-nil, matching Kind.
type Record struct {
	Kind       Kind
	Message    *Message
	RoomUpdate *RoomUpdate
	Activity   *Activity
}

// ServerInfo is the server push sent on connect.
type ServerInfo struct {
	Apron        int      `json:"apron"`
	Agent        string   `json:"agent"`
	Capabilities []string `json:"capabilities"`
	// Ping is the keepalive interval the server suggests, in seconds.
	// Zero when absent; the client then falls back to 30s.
	Ping time.Duration
}

func decodeServerInfo(data []byte) (ServerInfo, error) {
	var raw struct {
		Apron        int     `json:"apron"`
		Agent        string  `json:"agent"`
		Capabilities []string `json:"capabilities"`
		Ping         float64 `json:"ping"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return ServerInfo{}, err
	}
	info := ServerInfo{
		Apron:        raw.Apron,
		Agent:        raw.Agent,
		Capabilities: raw.Capabilities,
	}
	if raw.Ping > 0 {
		info.Ping = time.Duration(raw.Ping * float64(time.Second))
	}
	return info, nil
}

// Message is an inbound chat message push. A delete tombstone drops Body
// and Ext; at equal LogID a tombstone wins over the message it replaces.
type Message struct {
	MessageID string    `json:"message_id"`
	LogID     int64     `json:"log_id"`
	PrevLogID *int64    `json:"prev_log_id"`
	RoomID    string    `json:"room_id"`
	From      From      `json:"from"`
	// Body is nil for a delete tombstone.
	Body *Body `json:"body"`
	// ReplyTo, when present, links the message to the one it replies to.
	ReplyTo *ReplyTo `json:"reply_to"`
	// Deleted is set on a delete tombstone.
	Deleted bool `json:"deleted"`
	// Ext is nil for a delete tombstone.
	Ext json.RawMessage `json:"ext"`
}

// From identifies the author of a message.
type From struct {
	UserID string `json:"user_id"`
	Name   string `json:"name"`
}

// Body is the content of a message.
type Body struct {
	Text     string          `json:"text"`
	Mentions json.RawMessage `json:"mentions,omitempty"`
	Embeds   json.RawMessage `json:"embeds,omitempty"`
}

// ReplyTo links a message or post to the message it replies to.
type ReplyTo struct {
	MessageID string `json:"message_id"`
}

// RoomUpdate is a room_update push. Kind is one of "joined", "left",
// "updated", "memberships"; Raw carries the full payload.
type RoomUpdate struct {
	Kind string
	Raw  json.RawMessage
}

// Activity is an activity push; the payload is open-ended.
type Activity struct {
	Raw json.RawMessage
}

// decodePush decodes an inbound push. Unknown methods return ok=false;
// they are ignored, not errors.
func decodePush(method string, data []byte) (Record, bool) {
	switch method {
	case "message":
		var m Message
		if err := json.Unmarshal(data, &m); err != nil {
			return Record{}, false
		}
		return Record{Kind: KindMessage, Message: &m}, true
	case "room_update":
		var ru RoomUpdate
		if err := json.Unmarshal(data, &ru); err != nil {
			return Record{}, false
		}
		ru.Raw = data
		return Record{Kind: KindRoomUpdate, RoomUpdate: &ru}, true
	case "activity":
		return Record{Kind: KindActivity, Activity: &Activity{Raw: data}}, true
	default:
		return Record{}, false
	}
}

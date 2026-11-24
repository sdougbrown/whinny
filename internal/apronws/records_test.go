package apronws

import (
	"encoding/json"
	"testing"
)

func TestDecodePush(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		data     string
		wantKind Kind
		wantOK   bool
		check    func(t *testing.T, rec Record)
	}{
		{
			name:   "message",
			method: "message",
			data: `{"message_id":"m1","log_id":5,"prev_log_id":4,"room_id":"r1",
				"from":{"user_id":"u1","name":"Ann"},
				"body":{"text":"hello","mentions":["u2"],"embeds":[{"kind":"image"}]},
				"reply_to":{"message_id":"m0"},
				"ext":{"a":1}}`,
			wantKind: KindMessage,
			wantOK:   true,
			check: func(t *testing.T, rec Record) {
				m := rec.Message
				if m.MessageID != "m1" {
					t.Errorf("MessageID = %q, want m1", m.MessageID)
				}
				if m.LogID != 5 {
					t.Errorf("LogID = %d, want 5", m.LogID)
				}
				if m.PrevLogID == nil || *m.PrevLogID != 4 {
					t.Errorf("PrevLogID = %v, want 4", m.PrevLogID)
				}
				if m.RoomID != "r1" {
					t.Errorf("RoomID = %q, want r1", m.RoomID)
				}
				if m.From.UserID != "u1" || m.From.Name != "Ann" {
					t.Errorf("From = %+v, want u1/Ann", m.From)
				}
				if m.Body == nil || m.Body.Text != "hello" {
					t.Fatalf("Body = %+v, want text hello", m.Body)
				}
				if m.ReplyTo == nil || m.ReplyTo.MessageID != "m0" {
					t.Errorf("ReplyTo = %+v, want m0", m.ReplyTo)
				}
				if m.Deleted {
					t.Error("Deleted = true, want false")
				}
				if m.Ext == nil {
					t.Error("Ext = nil, want non-nil")
				}
			},
		},
		{
			// A tombstone at the same log_id as the message above: body and
			// ext are dropped, and the tombstone wins.
			name:     "tombstone at equal log_id",
			method:   "message",
			data:     `{"message_id":"m1","log_id":5,"room_id":"r1","from":{"user_id":"u1","name":"Ann"},"deleted":true}`,
			wantKind: KindMessage,
			wantOK:   true,
			check: func(t *testing.T, rec Record) {
				m := rec.Message
				if !m.Deleted {
					t.Error("Deleted = false, want true")
				}
				if m.Body != nil {
					t.Errorf("Body = %+v, want nil", m.Body)
				}
				if m.Ext != nil {
					t.Errorf("Ext = %s, want nil", m.Ext)
				}
				if m.ReplyTo != nil {
					t.Errorf("ReplyTo = %+v, want nil", m.ReplyTo)
				}
				if m.PrevLogID != nil {
					t.Errorf("PrevLogID = %v, want nil", m.PrevLogID)
				}
			},
		},
		{
			// §2.1: a log_id is a decimal string of Unix epoch milliseconds
			// on the wire; the decoder accepts the string shape and a bare
			// number.
			name:   "message with decimal-string log_id",
			method: "message",
			data: `{"message_id":"1724803200042","log_id":"1724803200042","prev_log_id":"1724803200041","room_id":"r1",
				"from":{"user_id":"u1","name":"Ann"},
				"body":{"text":"hello"}}`,
			wantKind: KindMessage,
			wantOK:   true,
			check: func(t *testing.T, rec Record) {
				m := rec.Message
				if m.LogID != 1724803200042 {
					t.Errorf("LogID = %d, want 1724803200042", m.LogID)
				}
				if m.PrevLogID == nil || *m.PrevLogID != 1724803200041 {
					t.Errorf("PrevLogID = %v, want 1724803200041", m.PrevLogID)
				}
			},
		},
		{
			name:     "message with an unparseable log_id",
			method:   "message",
			data:     `{"message_id":"m1","log_id":"soon","room_id":"r1","from":{"user_id":"u1"}}`,
			wantKind: 0,
			wantOK:   false,
		},
		{
			name:     "room_update",
			method:   "room_update",
			data:     `{"kind":"joined","room_id":"r2"}`,
			wantKind: KindRoomUpdate,
			wantOK:   true,
			check: func(t *testing.T, rec Record) {
				if rec.RoomUpdate.Kind != "joined" {
					t.Errorf("Kind = %q, want joined", rec.RoomUpdate.Kind)
				}
				if rec.RoomUpdate.Raw == nil {
					t.Error("Raw = nil, want non-nil")
				}
			},
		},
		{
			name:     "activity",
			method:   "activity",
			data:     `{"kind":"typing","user_id":"u2"}`,
			wantKind: KindActivity,
			wantOK:   true,
			check: func(t *testing.T, rec Record) {
				if rec.Activity.Raw == nil {
					t.Error("Raw = nil, want non-nil")
				}
			},
		},
		{
			name:   "unknown method ignored",
			method: "frobnicate",
			data:   `{}`,
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, ok := decodePush(tt.method, []byte(tt.data))
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if rec.Kind != tt.wantKind {
				t.Fatalf("Kind = %v, want %v", rec.Kind, tt.wantKind)
			}
			if tt.check != nil {
				tt.check(t, rec)
			}
		})
	}
}

func TestDecodePushReactions(t *testing.T) {
	data := `{"log_id":42,"message_id":"m1","room_id":"r1",
		"reactions":[{"from":{"user_id":"u1","name":"Ann"},"emojis":["👍"]},
			{"from":{"user_id":"u2"},"emojis":["🎉",":party:"]}]}`
	rec, ok := decodePush("reactions", []byte(data))
	if !ok {
		t.Fatal("decodePush(reactions) returned ok=false")
	}
	if rec.Kind != KindReactions {
		t.Fatalf("Kind = %v, want KindReactions", rec.Kind)
	}
	r := rec.Reactions
	if r == nil {
		t.Fatal("Reactions = nil")
	}
	if r.LogID != 42 || r.MessageID != "m1" || r.RoomID != "r1" {
		t.Errorf("LogID/MessageID/RoomID = %d/%q/%q, want 42/m1/r1", r.LogID, r.MessageID, r.RoomID)
	}
	if len(r.Reactions) != 2 {
		t.Fatalf("len(Reactions) = %d, want 2", len(r.Reactions))
	}
	if r.Reactions[0].From.UserID != "u1" || len(r.Reactions[0].Emojis) != 1 || r.Reactions[0].Emojis[0] != "👍" {
		t.Errorf("Reactions[0] = %+v, want u1/👍", r.Reactions[0])
	}
	if r.Reactions[1].Emojis[1] != ":party:" {
		t.Errorf("Reactions[1].Emojis = %v, want :party: kept", r.Reactions[1].Emojis)
	}
	if string(r.Raw) == "" {
		t.Error("Raw not set")
	}
}

func TestDecodePushReactionsMalformed(t *testing.T) {
	if _, ok := decodePush("reactions", []byte(`{"log_id":"not-a-number"}`)); ok {
		t.Error("malformed reactions push decoded ok")
	}
	if _, ok := decodePush("reactions", []byte(`not json`)); ok {
		t.Error("non-JSON reactions push decoded ok")
	}
}

func TestDecodeHistoryReactions(t *testing.T) {
	// A history reply's reactions partition decodes into
	// HistoryResult.Reactions: each record is one author's complete
	// set at that log position, oldest first.
	raw := []byte(`{
		"messages": [{"message_id":"m1","log_id":"10","room_id":"r1","from":{"user_id":"u1"}}],
		"reactions": [{"log_id":"11","message_id":"m1","room_id":"r1",
			"reactions":[{"from":{"user_id":"u2","name":"Ann"},"emojis":["👍"]}]}],
		"latest_log_id": "11"
	}`)
	var r HistoryResult
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(r.Reactions) != 1 {
		t.Fatalf("reactions = %d, want 1", len(r.Reactions))
	}
	rec := r.Reactions[0]
	if rec.MessageID != "m1" || rec.LogID != 11 || rec.RoomID != "r1" {
		t.Errorf("record = %+v, want m1/11/r1", rec)
	}
	if len(rec.Reactions) != 1 || rec.Reactions[0].From.UserID != "u2" || rec.Reactions[0].Emojis[0] != "👍" {
		t.Errorf("sets = %+v, want u2/👍", rec.Reactions)
	}
	if len(r.Messages) != 1 {
		t.Errorf("messages = %d, want 1", len(r.Messages))
	}
}

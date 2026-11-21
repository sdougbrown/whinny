package apronws

import "testing"

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
			name:   "tombstone at equal log_id",
			method: "message",
			data:   `{"message_id":"m1","log_id":5,"room_id":"r1","from":{"user_id":"u1","name":"Ann"},"deleted":true}`,
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

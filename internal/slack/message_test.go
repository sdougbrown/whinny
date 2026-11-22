package slack

import (
	"encoding/json"
	"testing"
)

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestDecodeMessagePlain(t *testing.T) {
	m, err := DecodeMessage(mustJSON(t, map[string]any{
		"type": "message", "channel": "C1", "user": "U1", "text": "hi",
		"ts": "1.1", "thread_ts": "0.9",
	}))
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	want := Msg{RoomID: "C1", MessageID: "1.1", FromUserID: "U1", Text: "hi", ReplyTo: "0.9"}
	if m != want {
		t.Errorf("Msg = %+v, want %+v", m, want)
	}
}

func TestDecodeMessageBotMessage(t *testing.T) {
	m, err := DecodeMessage(mustJSON(t, map[string]any{
		"type": "message", "channel": "C1", "bot_id": "B1", "subtype": "bot_message",
		"text": "beep", "ts": "1.2",
	}))
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if m.BotID != "B1" || m.FromUserID != "" || m.Subtype != "bot_message" {
		t.Errorf("Msg = %+v", m)
	}
}

func TestDecodeMessageChangedUnwraps(t *testing.T) {
	inner := map[string]any{
		"type": "message", "user": "U1", "text": "hi (edited)", "ts": "1.1", "thread_ts": "0.9",
	}
	m, err := DecodeMessage(mustJSON(t, map[string]any{
		"type": "message", "channel": "C1", "subtype": "message_changed",
		"message":          inner,
		"previous_message": map[string]any{"type": "message", "user": "U1", "text": "hi", "ts": "1.1"},
	}))
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if !m.Edited {
		t.Errorf("Edited = false, want true")
	}
	if m.RoomID != "C1" || m.MessageID != "1.1" || m.FromUserID != "U1" || m.Text != "hi (edited)" || m.ReplyTo != "0.9" {
		t.Errorf("Msg = %+v", m)
	}
}

func TestDecodeMessageChangedMissingInner(t *testing.T) {
	_, err := DecodeMessage(mustJSON(t, map[string]any{
		"type": "message", "channel": "C1", "subtype": "message_changed",
	}))
	if err == nil {
		t.Fatal("DecodeMessage: want error for a wrapper with no inner message")
	}
}

func TestDecodeMessageDeleted(t *testing.T) {
	m, err := DecodeMessage(mustJSON(t, map[string]any{
		"type": "message", "channel": "C1", "subtype": "message_deleted",
		"ts": "2.0", "deleted_ts": "1.1",
	}))
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if m.DeletedTS != "1.1" || m.Subtype != "message_deleted" || m.RoomID != "C1" {
		t.Errorf("Msg = %+v", m)
	}
}

func TestDecodeMessageEditedMarker(t *testing.T) {
	m, err := DecodeMessage(mustJSON(t, map[string]any{
		"type": "message", "channel": "C1", "user": "U1", "text": "hi", "ts": "1.1",
		"edited": map[string]any{"user": "U1", "ts": "1.2"},
	}))
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if !m.Edited {
		t.Errorf("Edited = false, want true for a message with an edited marker")
	}
}

func TestDecodeMessageInvalid(t *testing.T) {
	if _, err := DecodeMessage(json.RawMessage(`{`)); err == nil {
		t.Fatal("DecodeMessage: want error for invalid JSON")
	}
}

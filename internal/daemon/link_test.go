package daemon

import (
	"encoding/json"
	"testing"

	"github.com/sdougbrown/whinny/internal/apronws"
)

func TestApronEventMessageMapping(t *testing.T) {
	rec := apronws.Record{Kind: apronws.KindMessage, Message: &apronws.Message{
		MessageID: "m1",
		LogID:     990001,
		RoomID:    "r1",
		From:      apronws.From{UserID: "u1", Name: "doug"},
		Body: &apronws.Body{
			Text:     "squire check the deploy",
			Mentions: json.RawMessage(`["u9"]`),
		},
		ReplyTo: &apronws.ReplyTo{MessageID: "m0"},
	}}
	ev, ok := apronEvent(rec)
	if !ok {
		t.Fatal("apronEvent dropped a message record")
	}
	if ev.Kind != KindMessage || ev.RoomID != "r1" || ev.Cursor != 990001 {
		t.Errorf("event = %+v, want kind message room r1 cursor 990001", ev)
	}
	if ev.Msg.MessageID != "m1" || ev.Msg.ReplyTo != "m0" || ev.Msg.From.UserID != "u1" {
		t.Errorf("msg = %+v", ev.Msg)
	}
	if len(ev.Msg.Mentions) != 1 || ev.Msg.Mentions[0] != "u9" {
		t.Errorf("mentions = %v, want [u9]", ev.Msg.Mentions)
	}
}

func TestApronEventImageURL(t *testing.T) {
	rec := apronws.Record{Kind: apronws.KindMessage, Message: &apronws.Message{
		MessageID: "m1",
		LogID:     990001,
		RoomID:    "r1",
		From:      apronws.From{UserID: "u1"},
		Body: &apronws.Body{
			Embeds: json.RawMessage(`[{"kind":"file","title":"notes"},{"kind":"image","url":"https://x/i.png"}]`),
		},
	}}
	ev, ok := apronEvent(rec)
	if !ok {
		t.Fatal("apronEvent dropped a message record")
	}
	if ev.ImageURL != "https://x/i.png" {
		t.Errorf("image URL = %q, want https://x/i.png", ev.ImageURL)
	}
	if len(ev.Msg.Embeds) != 2 || ev.Msg.Embeds[0].Title != "notes" {
		t.Errorf("embeds = %+v", ev.Msg.Embeds)
	}
}

func TestApronEventSkipsNonMessages(t *testing.T) {
	if _, ok := apronEvent(apronws.Record{Kind: apronws.KindRoomUpdate}); ok {
		t.Error("room update kept")
	}
	if _, ok := apronEvent(apronws.Record{Kind: apronws.KindActivity}); ok {
		t.Error("activity kept")
	}
	tomb := apronws.Record{Kind: apronws.KindMessage, Message: &apronws.Message{
		MessageID: "m1", LogID: 990001, RoomID: "r1", Deleted: true,
	}}
	if _, ok := apronEvent(tomb); ok {
		t.Error("delete tombstone kept")
	}
}

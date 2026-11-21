package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/sdougbrown/whinny/internal/apronws"
)

func TestResolveRoom(t *testing.T) {
	joined := []apronws.Room{
		{ID: "r1", Title: "General"},
		{ID: "r2", Title: "Random"},
		{ID: "r4", Title: "Dup"},
		{ID: "r5", Title: "Dup"},
	}
	notJoined := []apronws.Room{
		{ID: "r3", Title: "General"},
	}
	cases := []struct {
		name    string
		query   string
		wantID  string
		wantErr string
	}{
		{"exact id joined", "r1", "r1", ""},
		{"exact id not joined", "r3", "r3", ""},
		{"title case-insensitive", "general", "r1", ""},
		{"title upper", "RANDOM", "r2", ""},
		{"no match", "nope", "", `no room matches "nope"`},
		{"ambiguous", "Dup", "", "multiple rooms match"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveRoom(joined, notJoined, tc.query)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ResolveRoom = %v, want error containing %q", got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %q, want containing %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveRoom: %v", err)
			}
			if got.ID != tc.wantID {
				t.Fatalf("ResolveRoom id = %q, want %q", got.ID, tc.wantID)
			}
		})
	}
}

// A title that matches a not-joined room only must not resolve: the
// title ladder is joined rooms only.
func TestResolveRoomTitleExcludesNotJoined(t *testing.T) {
	notJoined := []apronws.Room{{ID: "r3", Title: "Solo"}}
	_, err := ResolveRoom(nil, notJoined, "solo")
	var re *RoomError
	if !errors.As(err, &re) {
		t.Fatalf("error = %v, want *RoomError", err)
	}
	if re.Kind != RoomNoMatch {
		t.Fatalf("RoomError kind = %v, want RoomNoMatch", re.Kind)
	}
}

func TestResolveRoomAmbiguousCarriesCandidates(t *testing.T) {
	joined := []apronws.Room{{ID: "a", Title: "Dup"}, {ID: "b", Title: "Dup"}}
	_, err := ResolveRoom(joined, nil, "dup")
	var re *RoomError
	if !errors.As(err, &re) {
		t.Fatalf("error = %v, want *RoomError", err)
	}
	if re.Kind != RoomAmbiguous || len(re.Rooms) != 2 {
		t.Fatalf("RoomError = %+v, want ambiguous with 2 candidates", re)
	}
	if re.Rooms[0].ID != "a" || re.Rooms[1].ID != "b" {
		t.Fatalf("candidates = %v, want [a b]", re.Rooms)
	}
	if !strings.Contains(re.Error(), "disambiguate") {
		t.Fatalf("error = %q, want a disambiguate message", re.Error())
	}
}

// An exact id wins over a title match: a query equal to one room's id
// and another room's title resolves to the id.
func TestResolveRoomIDBeatsTitle(t *testing.T) {
	joined := []apronws.Room{
		{ID: "general", Title: "Other"},
		{ID: "r2", Title: "general"},
	}
	got, err := ResolveRoom(joined, nil, "general")
	if err != nil {
		t.Fatalf("ResolveRoom: %v", err)
	}
	if got.ID != "general" {
		t.Fatalf("ResolveRoom id = %q, want the exact id", got.ID)
	}
}

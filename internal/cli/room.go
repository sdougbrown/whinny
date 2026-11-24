package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/sdougbrown/whinny/internal/apronws"
)

// RoomClient is the apron surface the CLI commands use; *apronws.Client
// satisfies it.
type RoomClient interface {
	PostMessage(ctx context.Context, p apronws.PostParams) (apronws.PostResult, error)
	History(ctx context.Context, p apronws.HistoryParams) (apronws.HistoryResult, error)
	RoomList(ctx context.Context, p apronws.RoomListParams) (apronws.RoomListResult, error)
	CreateThread(ctx context.Context, p apronws.CreateThreadParams) (apronws.Room, error)
	JoinRoom(ctx context.Context, roomID string) error
	// SetReactions sets the caller's complete emoji set on one message;
	// an empty set clears. A transport without reaction support returns
	// a usage error.
	SetReactions(ctx context.Context, messageID string, emojis []string) error
	// EmojiCatalog lists the custom emoji when the transport offers a
	// catalog; nil or empty when it does not.
	EmojiCatalog(ctx context.Context) (map[string]apronws.EmojiEntry, error)
	Close()
}

// RoomErrorKind discriminates a room-resolution failure.
type RoomErrorKind int

const (
	// RoomNoMatch: no room id or joined-room title matches the query.
	RoomNoMatch RoomErrorKind = iota
	// RoomAmbiguous: several joined rooms share the query's title.
	RoomAmbiguous
)

// RoomError is a typed room-resolution failure; both kinds are usage
// errors the caller can fix from the message.
type RoomError struct {
	Kind  RoomErrorKind
	Query string
	// Rooms carries the ambiguous candidates.
	Rooms []apronws.Room
}

func (e *RoomError) Error() string {
	switch e.Kind {
	case RoomNoMatch:
		return fmt.Sprintf("no room matches %q", e.Query)
	case RoomAmbiguous:
		var b strings.Builder
		fmt.Fprintf(&b, "multiple rooms match %q; disambiguate with a room id:", e.Query)
		for _, r := range e.Rooms {
			fmt.Fprintf(&b, "\n  %s  %s", r.ID, r.Title)
		}
		return b.String()
	}
	return "room resolution error"
}

// ResolveRoom resolves a room query (id or title) against the room list:
// exact room_id match first (joined or not), then exact case-insensitive
// title match among joined rooms. Zero matches returns a RoomNoMatch
// error; two or more title matches return a RoomAmbiguous error carrying
// the candidates.
func ResolveRoom(joined, notJoined []apronws.Room, query string) (apronws.Room, error) {
	for _, r := range joined {
		if r.ID == query {
			return r, nil
		}
	}
	for _, r := range notJoined {
		if r.ID == query {
			return r, nil
		}
	}
	var matches []apronws.Room
	for _, r := range joined {
		if r.Title != "" && strings.EqualFold(r.Title, query) {
			matches = append(matches, r)
		}
	}
	switch len(matches) {
	case 0:
		return apronws.Room{}, &RoomError{Kind: RoomNoMatch, Query: query}
	case 1:
		return matches[0], nil
	default:
		return apronws.Room{}, &RoomError{Kind: RoomAmbiguous, Query: query, Rooms: matches}
	}
}

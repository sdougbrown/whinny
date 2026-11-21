package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/apronws"
)

// RoomsHandler runs `whinny rooms`: it lists the joined and not-joined
// rooms as `id  title` lines, or JSON with --json.
func RoomsHandler(d Deps) clicontract.HandleFunc {
	return func(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
		client, owned, err := d.connect(ctx, inv)
		if err != nil {
			return nil, err
		}
		if owned {
			defer client.Close()
		}
		res, err := client.RoomList(ctx, apronws.RoomListParams{})
		if err != nil {
			return nil, OperationalErrorf("room list: %v", err)
		}
		if inv.Bools["json"] {
			return map[string]any{
				"joined":     nonNil(res.Joined),
				"not_joined": nonNil(res.NotJoined),
			}, nil
		}
		var b strings.Builder
		fmt.Fprintln(&b, "joined:")
		for _, r := range res.Joined {
			roomLine(&b, r)
		}
		fmt.Fprintln(&b, "not joined:")
		for _, r := range res.NotJoined {
			roomLine(&b, r)
		}
		return strings.TrimSuffix(b.String(), "\n"), nil
	}
}

func roomLine(b *strings.Builder, r apronws.Room) {
	fmt.Fprintf(b, "  %s", r.ID)
	if r.Title != "" {
		fmt.Fprintf(b, "  %s", r.Title)
	}
	b.WriteByte('\n')
}

// nonNil renders a nil room slice as an empty JSON array, not null.
func nonNil(rooms []apronws.Room) []apronws.Room {
	if rooms == nil {
		return []apronws.Room{}
	}
	return rooms
}

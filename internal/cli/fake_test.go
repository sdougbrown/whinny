package cli

import (
	"context"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/apronws"
)

// fakeClient is a deterministic RoomClient for handler tests.
type fakeClient struct {
	joined      []apronws.Room
	notJoined   []apronws.Room
	posts       []apronws.PostParams
	threads     []apronws.CreateThreadParams
	histories   []apronws.HistoryParams
	history     apronws.HistoryResult
	joinedRooms []string
	postID      string
	threadID    string
	closeCalls  int
}

func (f *fakeClient) PostMessage(_ context.Context, p apronws.PostParams) (apronws.PostResult, error) {
	f.posts = append(f.posts, p)
	return apronws.PostResult{MessageID: f.postID}, nil
}

func (f *fakeClient) History(_ context.Context, p apronws.HistoryParams) (apronws.HistoryResult, error) {
	f.histories = append(f.histories, p)
	return f.history, nil
}

func (f *fakeClient) RoomList(_ context.Context, _ apronws.RoomListParams) (apronws.RoomListResult, error) {
	return apronws.RoomListResult{Joined: f.joined, NotJoined: f.notJoined}, nil
}

func (f *fakeClient) CreateThread(_ context.Context, p apronws.CreateThreadParams) (apronws.Room, error) {
	f.threads = append(f.threads, p)
	return apronws.Room{ID: f.threadID, Title: p.Title, ParentRoomID: p.ParentRoomID}, nil
}

func (f *fakeClient) JoinRoom(_ context.Context, id string) error {
	f.joinedRooms = append(f.joinedRooms, id)
	return nil
}

func (f *fakeClient) Close() { f.closeCalls++ }

// testDeps builds Deps around a fake client with an empty env.
func testDeps(c *fakeClient) Deps {
	return Deps{Client: c, Env: func(string) string { return "" }}
}

func inv(command string, args []string, flags map[string]string, bools map[string]bool) clicontract.Invocation {
	return clicontract.Invocation{
		Command: command,
		Args:    args,
		Flags:   flags,
		Bools:   bools,
	}
}

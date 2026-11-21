package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/store"
)

// runAdapter mirrors cmd/whinny's adapter: usage-class failures exit 2,
// everything else exits operational (1).
type runAdapter struct{}

func (runAdapter) ErrorResult(command string, err error) any {
	return map[string]any{"command": command, "status": "error", "error": err.Error()}
}

func (runAdapter) Write(w io.Writer, result any) error {
	enc := json.NewEncoder(w)
	return enc.Encode(result)
}

func (runAdapter) ExitCode(_ clicontract.Invocation, _ any, handlerErr error) int {
	if handlerErr != nil {
		if e, ok := handlerErr.(*Error); ok && e.Class == ClassUsage {
			return 2
		}
		return clicontract.ExitOperational
	}
	return 0
}

func (runAdapter) Deadline(ctx context.Context, _ clicontract.Invocation) (context.Context, context.CancelFunc) {
	return ctx, func() {}
}

func (runAdapter) BeforeHandler(_ clicontract.Invocation, _ io.Writer) {}

func (runAdapter) Validate(_ clicontract.Invocation) error { return nil }

// runPost runs `whinny post --room r1 hello` through the real
// clicontract.Run path against the deps' temp store and returns the
// exit code and the stderr text.
func runPost(t *testing.T, d Deps) (int, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := clicontract.Run(App(d), runAdapter{}, []string{"post", "--room", "r1", "hello"}, &out, &errb)
	return code, errb.String()
}

// openPostDeps builds Deps around a temp store with an empty env and a
// fixed clock, for the exit-code pinning tests.
func openPostDeps(t *testing.T, now time.Time) (Deps, *store.Store) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return Deps{
		Store: s,
		Env:   func(string) string { return "" },
		Now:   func() time.Time { return now },
	}, s
}

// TestPostRefusalExitCodes pins the three typed denials through the
// real Run path: each exits non-zero and carries the denial text on
// stderr.
func TestPostRefusalExitCodes(t *testing.T) {
	now := time.Unix(2_000_000, 0)

	t.Run("breaker latched", func(t *testing.T) {
		d, s := openPostDeps(t, now)
		if err := s.SetBreaker(context.Background(), store.Breaker{Latched: true, Reason: "test"}); err != nil {
			t.Fatalf("SetBreaker: %v", err)
		}
		code, stderr := runPost(t, d)
		if code == 0 {
			t.Fatalf("exit = 0, want non-zero")
		}
		if !strings.Contains(stderr, "refused-breaker") {
			t.Fatalf("stderr = %q, want the refused-breaker denial text", stderr)
		}
	})

	t.Run("rate limit exhausted", func(t *testing.T) {
		d, s := openPostDeps(t, now)
		if err := s.SetRateState(context.Background(), 0, now); err != nil {
			t.Fatalf("SetRateState: %v", err)
		}
		code, stderr := runPost(t, d)
		if code == 0 {
			t.Fatalf("exit = 0, want non-zero")
		}
		if !strings.Contains(stderr, "refused-rate-limit") {
			t.Fatalf("stderr = %q, want the refused-rate-limit denial text", stderr)
		}
	})

	t.Run("quiet", func(t *testing.T) {
		d, s := openPostDeps(t, now)
		if err := s.SetMeta(context.Background(), MetaQuietUntil, "9999999999"); err != nil {
			t.Fatalf("SetMeta: %v", err)
		}
		code, stderr := runPost(t, d)
		if code == 0 {
			t.Fatalf("exit = 0, want non-zero")
		}
		if !strings.Contains(stderr, "refused-quiet") {
			t.Fatalf("stderr = %q, want the refused-quiet denial text", stderr)
		}
	})
}

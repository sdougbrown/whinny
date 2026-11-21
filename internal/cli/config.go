package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sdougbrown/whinny/internal/store"
)

// DefaultURL is the apron endpoint when neither flag nor env names one.
const DefaultURL = "ws://127.0.0.1:8070/ws"

// Config is the resolved apron endpoint and token.
type Config struct {
	URL   string
	Token string
}

// ResolveConfig resolves the apron endpoint and token. Precedence: the
// --url/--token flags, then WHINNY_URL/APRON_URL (url) and
// WHINNY_TOKEN/APRON_TOKEN (token). The url falls back to DefaultURL;
// a missing token is a usage error the caller can fix.
func ResolveConfig(urlFlag, tokenFlag string, env func(string) string) (Config, error) {
	url := urlFlag
	if url == "" {
		url = env("WHINNY_URL")
	}
	if url == "" {
		url = env("APRON_URL")
	}
	if url == "" {
		url = DefaultURL
	}
	token := tokenFlag
	if token == "" {
		token = env("WHINNY_TOKEN")
	}
	if token == "" {
		token = env("APRON_TOKEN")
	}
	if token == "" {
		return Config{}, UsageErrorf("no apron token: set WHINNY_TOKEN (or APRON_TOKEN) or pass --token")
	}
	return Config{URL: url, Token: token}, nil
}

// StorePath resolves the state database path: $WHINNY_DB, or
// ~/.local/share/whinny/state.db.
func StorePath(env func(string) string) (string, error) {
	if p := env("WHINNY_DB"); p != "" {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("whinny: resolve home dir: %w", err)
	}
	return filepath.Join(home, ".local", "share", "whinny", "state.db"), nil
}

// OpenStore opens (creating the directory on demand) the state database
// at StorePath.
func OpenStore(env func(string) string) (*store.Store, error) {
	path, err := StorePath(env)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("whinny: create store dir: %w", err)
	}
	s, err := store.Open(path)
	if err != nil {
		return nil, fmt.Errorf("whinny: open store %s: %w", path, err)
	}
	return s, nil
}

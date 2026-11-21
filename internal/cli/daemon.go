package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/daemon"
	"github.com/sdougbrown/whinny/internal/store"
	"github.com/sdougbrown/whinny/internal/wake"
)

// DaemonConfigPath is the config file the daemon command loads when
// --config and $WHINNY_CONFIG name nothing; a missing default file is
// not an error.
const DaemonConfigPath = ".config/whinny/daemon.json"

// DaemonHandler runs `whinny daemon`: a long-running process that holds
// the apron connection, evaluates the wake policy, and delivers wakes
// through the configured backend. SIGINT/SIGTERM cancel the run context
// and the process exits 0. Output mode is raw: the feed backend writes
// its JSON lines straight to stdout.
//
// Flags: --mode resident|shared, --deliver exec|resume|socket|feed,
// --exec-cmd <cmd>, --socket-url <url>, --config <path>.
func DaemonHandler(d Deps) clicontract.RawFunc {
	return func(ctx context.Context, inv clicontract.Invocation, stdout io.Writer) error {
		cfg, err := ResolveDaemonConfig(inv, d.env())
		if err != nil {
			return err
		}
		st, owned, err := d.daemonStore()
		if err != nil {
			return err
		}
		if owned {
			defer st.Close()
		}
		dlv, cleanup, err := daemon.NewDeliverer(cfg, stdout)
		if err != nil {
			return err
		}
		defer cleanup()

		// The app opts out of clicontract's signal wiring
		// (HandlerOwnedSignals): the daemon installs its own, so a
		// SIGINT/SIGTERM cancels the run context and exits 0.
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		run := d.RunDaemon
		if run == nil {
			run = daemon.Run
		}
		return run(ctx, cfg, daemon.Deps{Store: st, Deliver: dlv})
	}
}

// daemonStore opens the state store the daemon uses, reporting whether
// the caller owns it (and must close it).
func (d Deps) daemonStore() (*store.Store, bool, error) {
	return d.storeOwned()
}

// daemonFileConfig is the daemon config file's JSON shape: the
// non-secret parts. URL and token never come from the file — env and
// flags carry those.
type daemonFileConfig struct {
	Agent     string                    `json:"agent"`
	Mode      string                    `json:"mode"`
	Deliver   string                    `json:"deliver"`
	ExecCmd   []string                  `json:"exec_cmd"`
	SocketURL string                    `json:"socket_url"`
	Peers     map[string]daemonPeerJSON `json:"peers"`
	Self      daemonSelfJSON            `json:"self"`
	Ambient   daemonAmbientJSON         `json:"ambient"`
}

type daemonPeerJSON struct {
	Name  string `json:"name"`
	IsBot bool   `json:"is_bot"`
}

type daemonSelfJSON struct {
	UserID  string   `json:"user_id"`
	Name    string   `json:"name"`
	Aliases []string `json:"aliases"`
}

type daemonAmbientJSON struct {
	Endpoint   string  `json:"endpoint"`
	Threshold  float64 `json:"threshold"`
	MinSpacing string  `json:"min_spacing"`
	MaxPerHour int     `json:"max_per_hour"`
}

// ResolveDaemonConfig resolves the daemon configuration. Precedence, per
// field:
//
//   - URL/token: --url/--token flags, then $WHINNY_URL/$APRON_URL and
//     $WHINNY_TOKEN/$APRON_TOKEN, then the default endpoint.
//   - mode, deliver, exec-cmd, socket-url: the --flag, then
//     $WHINNY_DAEMON_MODE / $WHINNY_DAEMON_DELIVER, then the config
//     file, then the built-in default.
//   - agent, peers, self, ambient: the config file only (no flags).
//
// The config file path is --config, then $WHINNY_CONFIG, then
// ~/.config/whinny/daemon.json (optional: a missing default file is not
// an error; a missing explicit one is).
func ResolveDaemonConfig(inv clicontract.Invocation, env func(string) string) (daemon.Config, error) {
	path := inv.Flags["config"]
	if path == "" {
		path = env("WHINNY_CONFIG")
	}
	defaultPath := path == ""
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return daemon.Config{}, OperationalErrorf("resolve home dir: %v", err)
		}
		path = filepath.Join(home, DaemonConfigPath)
	}

	var cfg daemon.Config
	if !defaultPath {
		b, err := os.ReadFile(path)
		if err != nil {
			return daemon.Config{}, UsageErrorf("daemon: read config %s: %v", path, err)
		}
		cfg, err = parseDaemonConfig(b)
		if err != nil {
			return daemon.Config{}, UsageErrorf("daemon: config %s: %v", path, err)
		}
	} else if b, err := os.ReadFile(path); err == nil {
		cfg, err = parseDaemonConfig(b)
		if err != nil {
			return daemon.Config{}, UsageErrorf("daemon: config %s: %v", path, err)
		}
	} else if !os.IsNotExist(err) {
		// A default config that does not exist is fine; one that cannot
		// be read is a real problem and must not silently become
		// defaults.
		return daemon.Config{}, UsageErrorf("daemon: read config %s: %v", path, err)
	}

	endpoint, err := ResolveConfig(inv.Flags["url"], inv.Flags["token"], env)
	if err != nil {
		return daemon.Config{}, err
	}
	cfg.URL = endpoint.URL
	cfg.Token = endpoint.Token
	if p, err := StorePath(env); err == nil {
		cfg.DBPath = p
	}

	if v := inv.Flags["mode"]; v != "" {
		cfg.Mode = v
	} else if v := env("WHINNY_DAEMON_MODE"); v != "" {
		cfg.Mode = v
	}
	if v := inv.Flags["deliver"]; v != "" {
		cfg.Deliver = v
	} else if v := env("WHINNY_DAEMON_DELIVER"); v != "" {
		cfg.Deliver = v
	}
	if v := inv.Flags["exec-cmd"]; v != "" {
		cfg.ExecCmd = daemon.SplitCommand(v)
	}
	if v := inv.Flags["socket-url"]; v != "" {
		cfg.SocketURL = v
	}

	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return daemon.Config{}, UsageErrorf("daemon: %v", err)
	}
	if err := cfg.ValidateBackend(); err != nil {
		return daemon.Config{}, UsageErrorf("daemon: %v", err)
	}
	return cfg, nil
}

// parseDaemonConfig decodes the JSON config file.
func parseDaemonConfig(b []byte) (daemon.Config, error) {
	var fc daemonFileConfig
	if err := json.Unmarshal(b, &fc); err != nil {
		return daemon.Config{}, fmt.Errorf("parse: %w", err)
	}
	var minSpacing time.Duration
	if fc.Ambient.MinSpacing != "" {
		d, err := time.ParseDuration(fc.Ambient.MinSpacing)
		if err != nil {
			return daemon.Config{}, fmt.Errorf("ambient.min_spacing: %w", err)
		}
		minSpacing = d
	}
	cfg := daemon.Config{
		Agent:     fc.Agent,
		Mode:      fc.Mode,
		Deliver:   fc.Deliver,
		ExecCmd:   fc.ExecCmd,
		SocketURL: fc.SocketURL,
		Self: wake.Self{
			UserID:  fc.Self.UserID,
			Name:    fc.Self.Name,
			Aliases: fc.Self.Aliases,
		},
		Ambient: daemon.AmbientConfig{
			Endpoint:   fc.Ambient.Endpoint,
			Threshold:  fc.Ambient.Threshold,
			MinSpacing: minSpacing,
			MaxPerHour: fc.Ambient.MaxPerHour,
		},
	}
	if len(fc.Peers) > 0 {
		cfg.Peers = make(wake.Peers, len(fc.Peers))
		for id, p := range fc.Peers {
			cfg.Peers[id] = wake.Peer{Name: p.Name, IsBot: p.IsBot}
		}
	}
	return cfg, nil
}

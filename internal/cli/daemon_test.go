package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/daemon"
	"github.com/sdougbrown/whinny/internal/store"
)

func testInvocation(flags map[string]string) clicontract.Invocation {
	return clicontract.Invocation{Command: "daemon", Flags: flags}
}

func TestResolveDaemonConfig(t *testing.T) {
	env := func(k string) string {
		switch k {
		case "WHINNY_TOKEN":
			return "tok"
		case "WHINNY_URL":
			return "ws://apron.test/ws"
		default:
			return ""
		}
	}

	t.Run("file supplies the non-secret parts", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "daemon.json")
		os.WriteFile(path, []byte(`{
			"agent": "squire",
			"mode": "shared",
			"deliver": "socket",
			"socket_url": "http://127.0.0.1:9/post",
			"peers": {"u1": {"name": "Doug", "is_bot": false}, "u2": {"name": "umpire", "is_bot": true}},
			"self": {"user_id": "u9", "name": "squire", "aliases": ["sq"]},
			"ambient": {"endpoint": "http://classify.test", "threshold": 0.7, "min_spacing": "5m", "max_per_hour": 3}
		}`), 0o644)
		cfg, err := ResolveDaemonConfig(testInvocation(map[string]string{"config": path}), env)
		if err != nil {
			t.Fatalf("ResolveDaemonConfig: %v", err)
		}
		if cfg.Agent != "squire" || cfg.Mode != "shared" || cfg.Deliver != "socket" || cfg.SocketURL != "http://127.0.0.1:9/post" {
			t.Errorf("config = %+v", cfg)
		}
		if cfg.Self.UserID != "u9" || cfg.Self.Name != "squire" || strings.Join(cfg.Self.Aliases, ",") != "sq" {
			t.Errorf("self = %+v", cfg.Self)
		}
		if cfg.Peers["u2"].IsBot != true || cfg.Peers["u1"].IsBot != false {
			t.Errorf("peers = %+v", cfg.Peers)
		}
		if cfg.Ambient.Endpoint != "http://classify.test" || cfg.Ambient.Threshold != 0.7 ||
			cfg.Ambient.MinSpacing != 5*60*1e9 || cfg.Ambient.MaxPerHour != 3 {
			t.Errorf("ambient = %+v", cfg.Ambient)
		}
		if cfg.URL != "ws://apron.test/ws" || cfg.Token != "tok" {
			t.Errorf("endpoint = %s token = %q", cfg.URL, cfg.Token)
		}
		if cfg.DBPath == "" {
			t.Error("DBPath not resolved")
		}
	})

	t.Run("flags override the file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "daemon.json")
		os.WriteFile(path, []byte(`{"mode": "shared", "deliver": "exec", "exec_cmd": ["old"]}`), 0o644)
		inv := testInvocation(map[string]string{
			"config": path, "mode": "resident", "deliver": "exec",
			"exec-cmd": `opencode run --flag "two words"`, "socket-url": "http://x",
		})
		cfg, err := ResolveDaemonConfig(inv, env)
		if err != nil {
			t.Fatalf("ResolveDaemonConfig: %v", err)
		}
		if cfg.Mode != "resident" {
			t.Errorf("mode = %q, want resident (flag wins)", cfg.Mode)
		}
		if strings.Join(cfg.ExecCmd, "|") != "opencode|run|--flag|two words" {
			t.Errorf("exec cmd = %q", cfg.ExecCmd)
		}
	})

	t.Run("env overrides the file for mode and deliver", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "daemon.json")
		os.WriteFile(path, []byte(`{"mode": "resident"}`), 0o644)
		e := func(k string) string {
			if k == "WHINNY_DAEMON_MODE" {
				return "shared"
			}
			if k == "WHINNY_TOKEN" {
				return "tok"
			}
			if k == "WHINNY_DAEMON_DELIVER" {
				return "feed"
			}
			return ""
		}
		cfg, err := ResolveDaemonConfig(testInvocation(map[string]string{"config": path}), e)
		if err != nil {
			t.Fatalf("ResolveDaemonConfig: %v", err)
		}
		if cfg.Mode != "shared" {
			t.Errorf("mode = %q, want shared (env wins)", cfg.Mode)
		}
		if cfg.Deliver != "feed" {
			t.Errorf("deliver = %q, want feed (env wins)", cfg.Deliver)
		}
	})

	t.Run("defaults when the file is absent", func(t *testing.T) {
		cfg, err := ResolveDaemonConfig(testInvocation(map[string]string{"exec-cmd": "run-wake"}), env)
		if err != nil {
			t.Fatalf("ResolveDaemonConfig: %v", err)
		}
		if cfg.Agent != "resident" || cfg.Mode != "resident" || cfg.Deliver != "exec" {
			t.Errorf("config = %+v, want resident defaults", cfg)
		}
		if strings.Join(cfg.ExecCmd, "|") != "run-wake" {
			t.Errorf("exec cmd = %q", cfg.ExecCmd)
		}
	})

	t.Run("explicit missing config is a usage error", func(t *testing.T) {
		_, err := ResolveDaemonConfig(testInvocation(map[string]string{"config": "/nonexistent/daemon.json"}), env)
		if !isUsageErr(err) {
			t.Errorf("err = %v, want usage error", err)
		}
	})

	t.Run("invalid mode is a usage error", func(t *testing.T) {
		_, err := ResolveDaemonConfig(testInvocation(map[string]string{"mode": "bogus"}), env)
		if !isUsageErr(err) {
			t.Errorf("err = %v, want usage error", err)
		}
	})

	t.Run("exec deliver without a command is a usage error", func(t *testing.T) {
		_, err := ResolveDaemonConfig(testInvocation(map[string]string{"deliver": "exec"}), env)
		if !isUsageErr(err) {
			t.Errorf("err = %v, want usage error", err)
		}
	})

	t.Run("socket deliver without a URL is a usage error", func(t *testing.T) {
		_, err := ResolveDaemonConfig(testInvocation(map[string]string{"deliver": "socket"}), env)
		if !isUsageErr(err) {
			t.Errorf("err = %v, want usage error", err)
		}
	})
}

// TestResolveDaemonConfigModeDeliverPrecedence pins the mode/deliver
// precedence: a flag beats the config file, and env beats the config
// file when no flag is set.
func TestResolveDaemonConfigModeDeliverPrecedence(t *testing.T) {
	env := func(k string) string {
		switch k {
		case "WHINNY_TOKEN":
			return "tok"
		case "WHINNY_URL":
			return "ws://apron.test/ws"
		default:
			return ""
		}
	}

	t.Run("flag beats the file for mode", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "daemon.json")
		os.WriteFile(path, []byte(`{"mode": "shared", "exec_cmd": ["x"]}`), 0o644)
		cfg, err := ResolveDaemonConfig(testInvocation(map[string]string{"config": path, "mode": "resident"}), env)
		if err != nil {
			t.Fatalf("ResolveDaemonConfig: %v", err)
		}
		if cfg.Mode != "resident" {
			t.Errorf("mode = %q, want resident (flag wins)", cfg.Mode)
		}
	})

	t.Run("env beats the file for deliver with no flag", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "daemon.json")
		os.WriteFile(path, []byte(`{"deliver": "exec", "exec_cmd": ["old"]}`), 0o644)
		e := func(k string) string {
			if k == "WHINNY_DAEMON_DELIVER" {
				return "feed"
			}
			return env(k)
		}
		cfg, err := ResolveDaemonConfig(testInvocation(map[string]string{"config": path}), e)
		if err != nil {
			t.Fatalf("ResolveDaemonConfig: %v", err)
		}
		if cfg.Deliver != "feed" {
			t.Errorf("deliver = %q, want feed (env wins)", cfg.Deliver)
		}
	})
}

// isUsageErr reports whether err is a caller error: a parse failure or
// a usage-class cli.Error.
func isUsageErr(err error) bool {
	if _, ok := err.(*clicontract.UsageError); ok {
		return true
	}
	if e, ok := err.(*Error); ok {
		return e.Class == ClassUsage
	}
	return false
}

func TestDaemonCommandDeclared(t *testing.T) {
	app := App(Deps{})
	var cmd *clicontract.Command
	for _, c := range app.Commands {
		if c.Name == "daemon" {
			cmd = c
			break
		}
	}
	if cmd == nil {
		t.Fatal("no daemon command declared")
	}
	flags := map[string]bool{}
	for _, f := range cmd.Flags {
		flags[f.Name] = true
	}
	for _, want := range []string{"mode", "deliver", "exec-cmd", "socket-url", "config"} {
		if !flags[want] {
			t.Errorf("daemon command missing flag %q", want)
		}
	}
	if cmd.Output.Mode != clicontract.OutputRaw {
		t.Errorf("daemon output mode = %q, want raw", cmd.Output.Mode)
	}
	if err := clicontract.Validate(app); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestDaemonHandlerWiring(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	var gotCfg daemon.Config
	var gotDeps daemon.Deps
	ran := false
	d := Deps{
		Store: st,
		Env: func(k string) string {
			if k == "WHINNY_TOKEN" {
				return "tok"
			}
			return ""
		},
		RunDaemon: func(_ context.Context, cfg daemon.Config, deps daemon.Deps) error {
			ran = true
			gotCfg, gotDeps = cfg, deps
			return nil
		},
	}
	handler := DaemonHandler(d)
	var out bytes.Buffer
	inv := testInvocation(map[string]string{"mode": "shared", "deliver": "feed"})
	if err := handler(context.Background(), inv, &out); err != nil {
		t.Fatalf("daemon handler: %v", err)
	}
	if !ran {
		t.Fatal("RunDaemon not called")
	}
	if gotCfg.Mode != "shared" || gotCfg.Deliver != "feed" {
		t.Errorf("cfg = %+v", gotCfg)
	}
	if gotDeps.Store != st {
		t.Errorf("deps store not wired")
	}
	if gotDeps.Deliver == nil {
		t.Error("deps deliverer not wired")
	}
}

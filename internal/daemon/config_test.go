package daemon

import (
	"strings"
	"testing"
	"time"
)

func TestValidateRequiresSelfUserID(t *testing.T) {
	// An empty self identity is rejected in both modes: the firebreak
	// would match every post and the daemon would self-wake.
	cfg := Config{Mode: ModeResident, Deliver: DeliverExec}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "user_id") {
		t.Errorf("Validate = %v, want an error naming self.user_id", err)
	}
	// A valid user id passes in both modes (shared mode uses the same
	// identity for self-post subscription).
	cfg.Self.UserID = "u9"
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate resident with user id = %v, want nil", err)
	}
	cfg.Mode = ModeShared
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate shared with user id = %v, want nil", err)
	}
}

func TestWithDefaultsAmbientValves(t *testing.T) {
	// Gate enabled, min spacing unset: it defaults to 30m, like the
	// other valves.
	cfg := Config{Ambient: AmbientConfig{Endpoint: "http://classify.test"}}
	got := cfg.WithDefaults()
	if got.Ambient.MinSpacing != DefaultMinSpacing {
		t.Errorf("MinSpacing = %v, want %v (the default)", got.Ambient.MinSpacing, DefaultMinSpacing)
	}
	if got.Ambient.Threshold != DefaultThreshold || got.Ambient.MaxPerHour != DefaultMaxPerHour {
		t.Errorf("valves = threshold %v maxPerHour %v, want the defaults", got.Ambient.Threshold, got.Ambient.MaxPerHour)
	}

	// An explicit min spacing is preserved.
	cfg = Config{Ambient: AmbientConfig{Endpoint: "http://classify.test", MinSpacing: 5 * time.Minute}}
	if got := cfg.WithDefaults(); got.Ambient.MinSpacing != 5*time.Minute {
		t.Errorf("MinSpacing = %v, want 5m (explicit preserved)", got.Ambient.MinSpacing)
	}

	// A negative min spacing is a config error: it defaults too.
	cfg = Config{Ambient: AmbientConfig{Endpoint: "http://classify.test", MinSpacing: -time.Minute}}
	if got := cfg.WithDefaults(); got.Ambient.MinSpacing != DefaultMinSpacing {
		t.Errorf("MinSpacing = %v, want %v (a negative defaults)", got.Ambient.MinSpacing, DefaultMinSpacing)
	}

	// Gate disabled: no gating values are needed.
	cfg = Config{}
	if got := cfg.WithDefaults(); got.Ambient.Endpoint != "" || got.Ambient.MinSpacing != 0 ||
		got.Ambient.Threshold != 0 || got.Ambient.MaxPerHour != 0 {
		t.Errorf("ambient = %+v, want all zero (the gate is disabled)", got.Ambient)
	}
}

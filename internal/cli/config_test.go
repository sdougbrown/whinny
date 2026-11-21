package cli

import (
	"strings"
	"testing"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestResolveConfig(t *testing.T) {
	full := map[string]string{
		"WHINNY_URL":   "ws://whinny:1/ws",
		"APRON_URL":    "ws://apron:2/ws",
		"WHINNY_TOKEN": "wtoken",
		"APRON_TOKEN":  "atoken",
	}
	onlyApron := map[string]string{
		"APRON_URL":   "ws://apron:2/ws",
		"APRON_TOKEN": "atoken",
	}
	tokenOnly := map[string]string{
		"WHINNY_TOKEN": "wtoken",
	}
	cases := []struct {
		name    string
		env     map[string]string
		url     string
		token   string
		wantURL string
		wantTok string
		wantErr string
	}{
		{"flags override env", full, "ws://flag/ws", "ftok", "ws://flag/ws", "ftok", ""},
		{"whinny env wins", full, "", "", "ws://whinny:1/ws", "wtoken", ""},
		{"apron env fallback", onlyApron, "", "", "ws://apron:2/ws", "atoken", ""},
		{"url default", tokenOnly, "", "", DefaultURL, "wtoken", ""},
		{"missing token", map[string]string{"WHINNY_URL": "ws://x/ws"}, "", "", "ws://x/ws", "", "no apron token"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveConfig(tc.url, tc.token, envOf(tc.env))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ResolveConfig = %+v, want error containing %q", got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %q, want containing %q", err.Error(), tc.wantErr)
				}
				e, ok := err.(*Error)
				if !ok || e.Class != ClassUsage {
					t.Fatalf("missing-token error = %v, want a ClassUsage *Error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveConfig: %v", err)
			}
			if got.URL != tc.wantURL {
				t.Fatalf("URL = %q, want %q", got.URL, tc.wantURL)
			}
			if got.Token != tc.wantTok {
				t.Fatalf("Token = %q, want %q", got.Token, tc.wantTok)
			}
		})
	}
}

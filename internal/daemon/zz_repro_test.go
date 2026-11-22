package daemon

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/sdougbrown/whinny/internal/slack"
)

func TestReproConnectionsOpenHang(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(os.Stderr, "REPRO: hit handler")
		w.Write([]byte(`{"ok":true,"url":"ws://127.0.0.1:1"}`))
	}))
	defer api.Close()
	c := slack.NewClient("xoxb-test", slack.WithBaseURL(api.URL))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url, err := c.ConnectionsOpen(ctx)
	fmt.Fprintln(os.Stderr, "REPRO: url=", url, "err=", err)
	_ = url
	_ = cancel
}

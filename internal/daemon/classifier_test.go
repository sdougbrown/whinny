package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/sdougbrown/whinny/internal/wake"
)

func TestHTTPClassifier(t *testing.T) {
	var bodyMu sync.Mutex
	gotBody := map[string]any{}
	mux := http.NewServeMux()
	mux.HandleFunc("/directed-p", func(w http.ResponseWriter, r *http.Request) {
		bodyMu.Lock()
		defer bodyMu.Unlock()
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.Write([]byte(`{"directed":false,"p":0.9}`))
	})
	mux.HandleFunc("/directed-p-low", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"directed":true,"p":0.2}`))
	})
	mux.HandleFunc("/directed-only", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"directed":true}`))
	})
	mux.HandleFunc("/plain", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"directed":false}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := &HTTPClassifier{Endpoint: srv.URL + "/directed-p", Threshold: 0.5, Client: srv.Client()}
	directed, err := c.IsDirected(context.Background(), wake.Msg{From: wake.From{UserID: "u1"}, Text: "hey"}, wake.Self{})
	if err != nil || !directed {
		t.Errorf("p=0.9: directed = %v, err = %v; want true, nil", directed, err)
	}
	bodyMu.Lock()
	if gotBody["from"] != "u1" || gotBody["text"] != "hey" {
		t.Errorf("request body = %v, want from u1 text hey", gotBody)
	}
	bodyMu.Unlock()

	c.Endpoint = srv.URL + "/directed-p-low"
	directed, err = c.IsDirected(context.Background(), wake.Msg{}, wake.Self{})
	if err != nil || directed {
		t.Errorf("p=0.2: directed = %v, err = %v; want false, nil", directed, err)
	}

	c.Endpoint = srv.URL + "/directed-only"
	directed, err = c.IsDirected(context.Background(), wake.Msg{}, wake.Self{})
	if err != nil || !directed {
		t.Errorf("directed-only: directed = %v, err = %v; want true, nil", directed, err)
	}

	c.Endpoint = srv.URL + "/plain"
	directed, err = c.IsDirected(context.Background(), wake.Msg{}, wake.Self{})
	if err != nil || directed {
		t.Errorf("plain false: directed = %v, err = %v; want false, nil", directed, err)
	}
}

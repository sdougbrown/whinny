package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecDelivererReceivesStdin(t *testing.T) {
	script := filepath.Join(t.TempDir(), "sink.sh")
	out := filepath.Join(t.TempDir(), "got.txt")
	writeScript(t, script, "#!/bin/sh\ncat >> "+out+"\n")
	d := &execDeliverer{cmd: []string{script}}
	env := Envelope{Text: "[wake kind=mention room=r1]\nhello", From: "u1"}
	if err := d.Deliver(context.Background(), "agent", env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read sink: %v", err)
	}
	if string(b) != env.Text {
		t.Errorf("stdin = %q, want %q", b, env.Text)
	}
}

func TestExecDelivererLogsNonZeroExit(t *testing.T) {
	script := filepath.Join(t.TempDir(), "fail.sh")
	writeScript(t, script, "#!/bin/sh\ncat > /dev/null\nexit 3\n")
	d := &execDeliverer{cmd: []string{script}}
	if err := d.Deliver(context.Background(), "agent", Envelope{Text: "x"}); err != nil {
		t.Errorf("Deliver with failing command: err = %v, want nil (logged, not raised)", err)
	}
}

func TestSocketDelivererPostsJSON(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	d := &socketDeliverer{url: srv.URL, client: srv.Client()}
	env := Envelope{Text: "[wake kind=watch id=w1]\nbody"}
	if err := d.Deliver(context.Background(), "squire", env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if got["text"] != env.Text || got["agent"] != "squire" {
		t.Errorf("POST body = %v, want text %q agent squire", got, env.Text)
	}
}

func TestFeedDelivererWritesJSONLine(t *testing.T) {
	var buf bytes.Buffer
	d := &feedDeliverer{out: &buf}
	env := Envelope{Text: "[wake kind=ambient room=r1]\nhi", From: "u7"}
	if err := d.Deliver(context.Background(), "agent", env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	var rec FeedRecord
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("decode %q: %v", buf.String(), err)
	}
	if rec.Text != env.Text || rec.From != "u7" || rec.Image != nil {
		t.Errorf("record = %+v, want text %q from u7 no image", rec, env.Text)
	}
}

func TestFeedDelivererFetchesImage(t *testing.T) {
	png := append([]byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}, bytes.Repeat([]byte{0}, 300)...)
	imgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// The header lies; the bytes are the truth.
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(png)
	}))
	defer imgSrv.Close()

	var buf bytes.Buffer
	d := &feedDeliverer{out: &buf}
	t.Cleanup(d.cleanup)
	env := Envelope{Text: "with image", From: "u1", ImageURL: imgSrv.URL}
	if err := d.Deliver(context.Background(), "agent", env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	var rec FeedRecord
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.Image == nil {
		t.Fatal("record has no image")
	}
	b, err := os.ReadFile(rec.Image.Path)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	if !bytes.Equal(b, png) {
		t.Errorf("image bytes = %d long, want %d", len(b), len(png))
	}
	if rec.Image.Media != "image/png" {
		t.Errorf("media = %q, want sniffed image/png", rec.Image.Media)
	}
}

func TestFeedDelivererImageTooLarge(t *testing.T) {
	big := bytes.Repeat([]byte{0xff, 0xd8, 0xff}, MaxImageBytes/3+10)
	imgSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(big)
	}))
	defer imgSrv.Close()

	var buf bytes.Buffer
	d := &feedDeliverer{out: &buf}
	t.Cleanup(d.cleanup)
	env := Envelope{Text: "big", From: "u1", ImageURL: imgSrv.URL}
	if err := d.Deliver(context.Background(), "agent", env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	// The text-only record still ships.
	var rec FeedRecord
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.Image != nil {
		t.Errorf("record carried an image %+v, want none", rec.Image)
	}
	if !strings.Contains(rec.Text, "big") {
		t.Errorf("record text = %q, want the envelope text", rec.Text)
	}
}

func TestSplitCommand(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"opencode run", []string{"opencode", "run"}},
		{`  spaced   out  `, []string{"spaced", "out"}},
		{`sh -c "cat >> a b"`, []string{"sh", "-c", "cat >> a b"}},
		{`sh -c 'echo it''s'`, []string{"sh", "-c", "echo its"}},
		{`unterminated "quote`, []string{"unterminated", "quote"}},
	}
	for _, tt := range tests {
		got := SplitCommand(tt.in)
		eq := len(got) == len(tt.want)
		if eq {
			for i := range got {
				if got[i] != tt.want[i] {
					eq = false
					break
				}
			}
		}
		if !eq {
			t.Errorf("SplitCommand(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

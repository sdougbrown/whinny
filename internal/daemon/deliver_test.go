package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestExecDelivererReturnsNonZeroExit(t *testing.T) {
	script := filepath.Join(t.TempDir(), "fail.sh")
	writeScript(t, script, "#!/bin/sh\ncat > /dev/null\nexit 3\n")
	d := &execDeliverer{cmd: []string{script}}
	err := d.Deliver(context.Background(), "agent", Envelope{Text: "x"})
	if err == nil {
		t.Errorf("Deliver with failing command: err = nil, want the exit error (callers decide to log or keep the wake)")
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
	u, _ := url.Parse(imgSrv.URL)
	d := &feedDeliverer{out: &buf, originScheme: u.Scheme, originHost: u.Host}
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
	u, _ := url.Parse(imgSrv.URL)
	d := &feedDeliverer{out: &buf, originScheme: u.Scheme, originHost: u.Host}
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

// TestFeedDelivererRefusesCrossOrigin: a cross-origin image URL is
// refused without a fetch (SSRF guard).
func TestFeedDelivererRefusesCrossOrigin(t *testing.T) {
	fetched := false
	otherSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetched = true
		w.Write([]byte("x"))
	}))
	defer otherSrv.Close()

	// The origin is a different host.
	originSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer originSrv.Close()
	ou, _ := url.Parse(originSrv.URL)

	var buf bytes.Buffer
	d := &feedDeliverer{out: &buf, originScheme: ou.Scheme, originHost: ou.Host}
	t.Cleanup(d.cleanup)
	env := Envelope{Text: "x", From: "u1", ImageURL: otherSrv.URL}
	if err := d.Deliver(context.Background(), "agent", env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if fetched {
		t.Error("cross-origin URL was fetched, want refused")
	}
	var rec FeedRecord
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.Image != nil {
		t.Errorf("record carried an image %+v, want none (refused)", rec.Image)
	}
}

// TestFeedDelivererRefusesCrossOriginRedirect: a same-origin fetch that
// redirects to a cross-origin URL is refused (the CheckRedirect guard).
func TestFeedDelivererRefusesCrossOriginRedirect(t *testing.T) {
	otherSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("x"))
	}))
	defer otherSrv.Close()

	// The origin server redirects to a cross-origin URL.
	originSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, otherSrv.URL, http.StatusFound)
	}))
	defer originSrv.Close()
	ou, _ := url.Parse(originSrv.URL)

	var buf bytes.Buffer
	d := &feedDeliverer{out: &buf, originScheme: ou.Scheme, originHost: ou.Host}
	t.Cleanup(d.cleanup)
	env := Envelope{Text: "x", From: "u1", ImageURL: originSrv.URL}
	if err := d.Deliver(context.Background(), "agent", env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	var rec FeedRecord
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.Image != nil {
		t.Errorf("record carried an image %+v, want none (cross-origin redirect refused)", rec.Image)
	}
}

// TestFeedDelivererFollowsSameOriginRedirect: a same-origin redirect is
// followed (the guard only refuses cross-origin).
func TestFeedDelivererFollowsSameOriginRedirect(t *testing.T) {
	png := append([]byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}, bytes.Repeat([]byte{0}, 100)...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/img", http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Write(png)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	var buf bytes.Buffer
	d := &feedDeliverer{out: &buf, originScheme: u.Scheme, originHost: u.Host}
	t.Cleanup(d.cleanup)
	env := Envelope{Text: "x", From: "u1", ImageURL: srv.URL + "/redirect"}
	if err := d.Deliver(context.Background(), "agent", env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	var rec FeedRecord
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rec.Image == nil {
		t.Fatal("record has no image (same-origin redirect should be followed)")
	}
}

// TestFeedDelivererTempDirCap: the image temp directory is capped at
// maxImageFiles; the oldest is evicted before a new one is written.
func TestFeedDelivererTempDirCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte{0x89, 'P', 'N', 'G'})
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	var buf bytes.Buffer
	d := &feedDeliverer{out: &buf, originScheme: u.Scheme, originHost: u.Host}
	t.Cleanup(d.cleanup)
	for i := 0; i < maxImageFiles+5; i++ {
		env := Envelope{Text: "x", From: "u1", ImageURL: srv.URL}
		if err := d.Deliver(context.Background(), "agent", env); err != nil {
			t.Fatalf("Deliver %d: %v", i, err)
		}
	}
	d.filesMu.Lock()
	n := len(d.files)
	d.filesMu.Unlock()
	if n != maxImageFiles {
		t.Errorf("files = %d, want %d (capped)", n, maxImageFiles)
	}
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) > maxImageFiles {
		t.Errorf("dir entries = %d, want <= %d", len(entries), maxImageFiles)
	}
}

// TestSlackFetcherSendsBearerAuth: the Slack fetcher sends the bot
// token as a bearer header and returns the bytes and media type.
func TestSlackFetcherSendsBearerAuth(t *testing.T) {
	png := append([]byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}, bytes.Repeat([]byte{0}, 100)...)
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "image/png")
		w.Write(png)
	}))
	defer srv.Close()

	f := newSlackFetcher("xoxb-test-token")
	f.hostAllowed = func(string) bool { return true }
	data, media, err := f.Fetch(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if gotAuth != "Bearer xoxb-test-token" {
		t.Errorf("authorization = %q, want the bot token bearer", gotAuth)
	}
	if !bytes.Equal(data, png) {
		t.Errorf("bytes = %d long, want %d", len(data), len(png))
	}
	if media != "image/png" {
		t.Errorf("media = %q, want image/png", media)
	}
}

// TestSlackFetcherRefusesOffOrigin: a URL off a slack.com host is
// refused without a fetch (SSRF guard).
func TestSlackFetcherRefusesOffOrigin(t *testing.T) {
	fetched := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetched = true
		w.Write([]byte("x"))
	}))
	defer srv.Close()

	f := newSlackFetcher("xoxb-test-token")
	if _, _, err := f.Fetch(context.Background(), srv.URL); err == nil {
		t.Error("off-origin fetch succeeded, want refused")
	}
	if fetched {
		t.Error("off-origin URL was fetched, want refused")
	}
}

// TestSlackFetcherRefusesOffOriginRedirect: a fetch that redirects off
// the allowed host is refused (the CheckRedirect guard).
func TestSlackFetcherRefusesOffOriginRedirect(t *testing.T) {
	otherFetched := false
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		otherFetched = true
		w.Write([]byte("x"))
	}))
	defer other.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusFound)
	}))
	defer origin.Close()

	f := newSlackFetcher("xoxb-test-token")
	// The origin host is allowed; the redirect target is not.
	ou, _ := url.Parse(origin.URL)
	f.hostAllowed = func(host string) bool { return host == ou.Host }
	if _, _, err := f.Fetch(context.Background(), origin.URL); err == nil {
		t.Error("off-origin redirect was followed, want refused")
	}
	if otherFetched {
		t.Error("redirect target was fetched, want refused")
	}
}

// TestFeedDelivererWithSlackFetcher: an injected Slack fetcher
// (instead of the apron fallback) fetches the embed with bearer auth
// and the record carries the image by path.
func TestFeedDelivererWithSlackFetcher(t *testing.T) {
	png := append([]byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}, bytes.Repeat([]byte{0}, 100)...)
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "image/png")
		w.Write(png)
	}))
	defer srv.Close()

	f := newSlackFetcher("xoxb-test-token")
	f.hostAllowed = func(string) bool { return true }
	var buf bytes.Buffer
	d := &feedDeliverer{out: &buf, fetch: f}
	t.Cleanup(d.cleanup)
	env := Envelope{Text: "with image", From: "u1", ImageURL: srv.URL}
	if err := d.Deliver(context.Background(), "agent", env); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if gotAuth != "Bearer xoxb-test-token" {
		t.Errorf("authorization = %q, want the bot token bearer", gotAuth)
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
		t.Errorf("media = %q, want image/png", rec.Image.Media)
	}
}

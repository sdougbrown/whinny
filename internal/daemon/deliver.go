package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Deliverer delivers one wake envelope to one agent.
type Deliverer interface {
	Deliver(ctx context.Context, agent string, env Envelope) error
}

// NewDeliverer builds the deliverer for cfg.Deliver. The feed backend
// writes JSON lines to stdout and fetches image embeds into a temp
// directory it owns; the returned cleanup removes that directory and is
// safe to call nil. exec and resume share the spawn shape: the envelope
// goes to the command's stdin.
func NewDeliverer(cfg Config, stdout io.Writer) (Deliverer, func(), error) {
	switch cfg.Deliver {
	case DeliverExec, DeliverResume:
		return &execDeliverer{cmd: cfg.ExecCmd}, func() {}, nil
	case DeliverSocket:
		return &socketDeliverer{url: cfg.SocketURL, client: &http.Client{Timeout: SocketTimeout}}, func() {}, nil
	case DeliverFeed:
		originScheme, originHost, err := apronOrigin(cfg.URL)
		if err != nil {
			return nil, nil, fmt.Errorf("daemon: derive apron origin: %w", err)
		}
		// The fetcher is per transport: apron is same-origin only, Slack
		// is bearer-authenticated and restricted to slack.com hosts.
		var fetch imageFetcher
		if cfg.Transport == TransportSlack {
			fetch = newSlackFetcher(cfg.BotToken)
		} else {
			fetch = newApronFetcher(originScheme, originHost)
		}
		f := &feedDeliverer{out: stdout, fetch: fetch, originScheme: originScheme, originHost: originHost}
		return f, func() { f.cleanup() }, nil
	default:
		return nil, nil, fmt.Errorf("daemon: unknown deliver backend %q", cfg.Deliver)
	}
}

// execDeliverer spawns cmd per wake, writes the envelope to stdin, and
// waits (bounded by ExecWaitTimeout). A non-zero exit or spawn failure is
// returned: the caller logs and moves on — the queue is already drained,
// and a failed delivery is at-most-once by the store's contract, but the
// shared expiry path needs to know to keep the watch for the next sweep.
type execDeliverer struct {
	cmd []string
}

func (d *execDeliverer) Deliver(ctx context.Context, _ string, env Envelope) error {
	ctx, cancel := context.WithTimeout(ctx, ExecWaitTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, d.cmd[0], d.cmd[1:]...)
	cmd.Stdin = strings.NewReader(env.Text)
	cmd.Stdout = io.Discard
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// socketDeliverer POSTs {"text": env, "agent": agent} to the socket URL.
// A failed POST is logged and dropped, like exec: the durable queue has
// already released the wake.
type socketDeliverer struct {
	url    string
	client *http.Client
}

func (d *socketDeliverer) Deliver(ctx context.Context, agent string, env Envelope) error {
	body, err := json.Marshal(map[string]string{"text": env.Text, "agent": agent})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("daemon: deliver via socket %s: %v", d.url, err)
		return nil
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("daemon: deliver via socket %s: status %d", d.url, resp.StatusCode)
	}
	return nil
}

// FeedRecord is one feed-backend delivery: a JSON line on the daemon's
// stdout, the poo feed contract's shape.
type FeedRecord struct {
	Text  string     `json:"text"`
	From  string     `json:"from"`
	Image *FeedImage `json:"image,omitempty"`
}

// FeedImage is a local file reference to a fetched image embed.
type FeedImage struct {
	Path  string `json:"path"`
	Media string `json:"media"`
}

// feedDeliverer writes FeedRecord JSON lines to stdout. When the
// triggering message carried an image embed (Envelope.ImageURL), the
// embed bytes are fetched into a per-run temp directory and referenced
// by path — never base64, never a URL for the consumer to fetch.
//
// The image fetch is SSRF-guarded by the transport's fetcher: apron is
// restricted to the apron origin (scheme+host) derived from the
// configured endpoint with cross-origin and cross-scheme redirects
// refused, Slack is restricted to slack.com hosts with the bot token's
// bearer auth. The fetch is bounded by a 5s timeout, and the temp
// directory is capped at maxImageFiles files (the oldest is evicted
// before a new one is written).
type feedDeliverer struct {
	out     io.Writer
	outMu   sync.Mutex // guards out: queue flushes are concurrent
	fetch   imageFetcher
	fetchMu sync.Mutex
	dir     string
	dirMu   sync.Mutex
	// originScheme/originHost build the fallback apron fetcher when no
	// fetcher was injected (tests build the deliverer without one).
	originScheme string
	originHost   string
	filesMu      sync.Mutex
	files        []string // FIFO order of written files
}

// maxImageFiles caps the image temp directory; the oldest file is
// evicted before a new one is written.
const maxImageFiles = 50

// imageFetchTimeout bounds one image fetch.
const imageFetchTimeout = 5 * time.Second

// apronOrigin derives the fetch origin (scheme+host) from the apron
// endpoint: a ws/wss scheme maps to http/https for the image fetch, and
// the host is the endpoint's host.
func apronOrigin(u string) (string, string, error) {
	p, err := url.Parse(u)
	if err != nil {
		return "", "", err
	}
	scheme := strings.ToLower(p.Scheme)
	switch scheme {
	case "ws":
		scheme = "http"
	case "wss":
		scheme = "https"
	}
	return scheme, p.Host, nil
}

// getFetcher returns the image fetcher, falling back to an apron
// fetcher built from the configured origin when none was injected.
func (d *feedDeliverer) getFetcher() imageFetcher {
	d.fetchMu.Lock()
	defer d.fetchMu.Unlock()
	if d.fetch == nil {
		d.fetch = newApronFetcher(d.originScheme, d.originHost)
	}
	return d.fetch
}

// getDir returns the image temp directory, creating it on first use.
func (d *feedDeliverer) getDir() (string, error) {
	d.dirMu.Lock()
	defer d.dirMu.Unlock()
	if d.dir == "" {
		dir, err := os.MkdirTemp("", "whinny-daemon-images-")
		if err != nil {
			return "", err
		}
		d.dir = dir
	}
	return d.dir, nil
}

func (d *feedDeliverer) Deliver(ctx context.Context, _ string, env Envelope) error {
	rec := FeedRecord{Text: env.Text, From: env.From}
	if env.ImageURL != "" {
		if img, err := d.fetchImage(ctx, env.ImageURL); err != nil {
			log.Printf("daemon: fetch image embed %s: %v", env.ImageURL, err)
		} else {
			rec.Image = img
		}
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	d.outMu.Lock()
	_, err = d.out.Write(append(b, '\n'))
	d.outMu.Unlock()
	return err
}

// fetchImage fetches url through the transport's fetcher (bounded by
// the 5s timeout at the call site), writes the bytes to the temp
// directory, and reports the media type: the response's Content-Type
// when it is image-specific, else a sniff of the first 512 bytes
// (headers lie).
func (d *feedDeliverer) fetchImage(ctx context.Context, rawURL string) (*FeedImage, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, imageFetchTimeout)
	defer cancel()
	body, media, err := d.getFetcher().Fetch(fetchCtx, rawURL)
	if err != nil {
		return nil, err
	}
	if media == "" || media == "application/octet-stream" {
		media = http.DetectContentType(body)
	}
	dir, err := d.getDir()
	if err != nil {
		return nil, err
	}
	// Cap the temp directory: evict the oldest file before writing a
	// new one.
	d.filesMu.Lock()
	if len(d.files) >= maxImageFiles {
		oldest := d.files[0]
		d.files = d.files[1:]
		os.Remove(oldest)
	}
	d.filesMu.Unlock()
	f, err := os.CreateTemp(dir, "embed-*")
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(body); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return nil, err
	}
	d.filesMu.Lock()
	d.files = append(d.files, f.Name())
	d.filesMu.Unlock()
	return &FeedImage{Path: f.Name(), Media: media}, nil
}

func (d *feedDeliverer) cleanup() {
	d.dirMu.Lock()
	dir := d.dir
	d.dirMu.Unlock()
	if dir != "" {
		os.RemoveAll(dir)
	}
}

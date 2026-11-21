package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
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
		f := &feedDeliverer{out: stdout, client: &http.Client{Timeout: SocketTimeout}}
		return f, func() { f.cleanup() }, nil
	default:
		return nil, nil, fmt.Errorf("daemon: unknown deliver backend %q", cfg.Deliver)
	}
}

// execDeliverer spawns cmd per wake, writes the envelope to stdin, and
// waits (bounded by ExecWaitTimeout). A non-zero exit or spawn failure is
// logged, not returned: the queue is already drained, and a failed
// delivery is at-most-once by the store's contract — the loop must keep
// going.
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
	if err := cmd.Run(); err != nil {
		log.Printf("daemon: deliver via exec %s: %v", d.cmd[0], err)
	}
	return nil
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
type feedDeliverer struct {
	out    io.Writer
	client *http.Client
	dir    string
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
	if _, err := d.out.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

// fetchImage fetches url, caps at MaxImageBytes, writes the bytes to the
// temp directory, and reports the media type: the response's
// Content-Type when it is image-specific, else a sniff of the first 512
// bytes (headers lie).
func (d *feedDeliverer) fetchImage(ctx context.Context, url string) (*FeedImage, error) {
	if d.client == nil {
		d.client = &http.Client{Timeout: SocketTimeout}
	}
	if d.dir == "" {
		dir, err := os.MkdirTemp("", "whinny-daemon-images-")
		if err != nil {
			return nil, err
		}
		d.dir = dir
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxImageBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > MaxImageBytes {
		return nil, fmt.Errorf("image exceeds %d byte cap", MaxImageBytes)
	}
	media := resp.Header.Get("Content-Type")
	if i := strings.Index(media, ";"); i >= 0 {
		media = media[:i]
	}
	media = strings.TrimSpace(media)
	if media == "" || media == "application/octet-stream" {
		media = http.DetectContentType(body)
	}
	f, err := os.CreateTemp(d.dir, "embed-*")
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
	return &FeedImage{Path: f.Name(), Media: media}, nil
}

func (d *feedDeliverer) cleanup() {
	if d.dir != "" {
		os.RemoveAll(d.dir)
	}
}

package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/sdougbrown/whinny/internal/slack"
	"strings"
)

// imageFetcher fetches one image URL and returns the bytes and the
// response's Content-Type (parameter stripped, trimmed). Each
// implementation owns its SSRF posture: the apron fetcher is
// same-origin only, the Slack fetcher is restricted to slack.com
// hosts and carries the bot token's bearer auth.
type imageFetcher interface {
	Fetch(ctx context.Context, rawURL string) (data []byte, mediaType string, err error)
}

// apronFetcher fetches images restricted to the apron origin (scheme +
// host) derived from the configured endpoint; cross-origin and
// cross-scheme redirects are refused.
type apronFetcher struct {
	client *http.Client
	scheme string
	host   string
}

// newApronFetcher builds the apron fetcher for the endpoint's origin.
func newApronFetcher(scheme, host string) *apronFetcher {
	return &apronFetcher{
		scheme: scheme,
		host:   host,
		client: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if req.URL.Scheme != scheme || req.URL.Host != host {
					return errors.New("refusing cross-origin redirect")
				}
				return nil
			},
		},
	}
}

// Fetch fetches rawURL, refusing any URL off the apron origin.
func (f *apronFetcher) Fetch(ctx context.Context, rawURL string) ([]byte, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, "", err
	}
	if f.host == "" || u.Scheme != f.scheme || u.Host != f.host {
		return nil, "", fmt.Errorf("refusing cross-origin image fetch %s (want %s://%s)", rawURL, f.scheme, f.host)
	}
	return fetchImageBytes(ctx, f.client, rawURL, nil)
}

// slackFetcher fetches Slack image embeds: allowed origins are
// slack.com hosts (slack.com, *.slack.com, e.g. files.slack.com),
// every request carries the bot token's bearer auth, and a redirect
// must stay on a slack.com host.
type slackFetcher struct {
	client *http.Client
	token  string
	// hostAllowed is the origin allow-list (the URL host, port
	// included); production is slack.HostAllowed.
	hostAllowed func(host string) bool
}

// newSlackFetcher builds the Slack fetcher for the bot token.
func newSlackFetcher(token string) *slackFetcher {
	f := &slackFetcher{token: token, hostAllowed: slack.HostAllowed}
	f.client = &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if !f.hostAllowed(req.URL.Host) {
				return errors.New("refusing redirect off slack.com")
			}
			return nil
		},
	}
	return f
}

// Fetch fetches rawURL, refusing any URL off a slack.com host.
func (f *slackFetcher) Fetch(ctx context.Context, rawURL string) ([]byte, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, "", err
	}
	if !f.hostAllowed(u.Host) {
		return nil, "", fmt.Errorf("refusing off-origin image fetch %s (want a slack.com host)", rawURL)
	}
	header := func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer "+f.token)
	}
	return fetchImageBytes(ctx, f.client, rawURL, header)
}

// fetchImageBytes performs one GET (with an optional per-request header
// setter), reads the body capped at MaxImageBytes, and returns the
// bytes and the response's Content-Type (parameter stripped, trimmed).
func fetchImageBytes(ctx context.Context, client *http.Client, rawURL string, setHeader func(*http.Request)) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", err
	}
	if setHeader != nil {
		setHeader(req)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxImageBytes+1))
	if err != nil {
		return nil, "", err
	}
	if len(body) > MaxImageBytes {
		return nil, "", fmt.Errorf("image exceeds %d byte cap", MaxImageBytes)
	}
	media := resp.Header.Get("Content-Type")
	if i := strings.Index(media, ";"); i >= 0 {
		media = media[:i]
	}
	return body, strings.TrimSpace(media), nil
}

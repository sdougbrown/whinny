package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/sdougbrown/whinny/internal/wake"
)

// HTTPClassifier is the ambient classifier client: POST JSON
// {"from": <user_id>, "text": <text>} to the endpoint, read
// {"directed": bool, "p": <0..1>} back. It satisfies wake.Classifier.
// Error handling (fail-open) is the wake package's: a transport error
// becomes one cheap no-op wake, never a dropped message.
type HTTPClassifier struct {
	// Endpoint is the classifier URL.
	Endpoint string
	// Threshold is the cutoff applied when the response carries p.
	Threshold float64
	// Client is the HTTP client; nil uses one with a 10s timeout.
	Client *http.Client
}

// IsDirected asks the classifier whether msg is directed at self.
func (c *HTTPClassifier) IsDirected(ctx context.Context, msg wake.Msg, _ wake.Self) (bool, error) {
	client := c.Client
	if client == nil {
		client = &http.Client{Timeout: ClassifierTimeout}
	}
	body, err := json.Marshal(map[string]string{
		"from": msg.From.UserID,
		"text": msg.Text,
	})
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("daemon: classifier %s: status %d", c.Endpoint, resp.StatusCode)
	}
	var out struct {
		Directed bool     `json:"directed"`
		P        *float64 `json:"p"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, err
	}
	if out.P != nil {
		return *out.P >= c.Threshold, nil
	}
	return out.Directed, nil
}

// classifierClient builds the classifier for the ambient gate, or nil
// when the gate is disabled.
func classifierClient(cfg Config) *HTTPClassifier {
	if cfg.Ambient.Endpoint == "" {
		return nil
	}
	return &HTTPClassifier{
		Endpoint:  cfg.Ambient.Endpoint,
		Threshold: cfg.Ambient.Threshold,
		Client:    &http.Client{Timeout: ClassifierTimeout},
	}
}

// clockFunc adapts a Now function to wake.Clock.
type clockFunc func() time.Time

func (f clockFunc) Now() time.Time { return f() }

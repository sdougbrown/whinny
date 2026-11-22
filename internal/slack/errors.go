package slack

import (
	"fmt"
	"time"
)

// APIError is a failed Slack Web API call: a Slack-level error from an
// ok:false response, a non-200 status, or a rate limit. The breaker
// feeds on RetryAfter.
type APIError struct {
	// Method is the Web API method that failed, e.g. "chat.postMessage".
	Method string
	// SlackError is Slack's "error" string from an ok:false response, or
	// "rate_limited" for HTTP 429, or "http_<status>" for other non-200
	// responses.
	SlackError string
	// RetryAfter is the wait Slack asked for, from the HTTP 429
	// Retry-After header (seconds). Zero when absent.
	RetryAfter time.Duration
}

// Error implements error.
func (e *APIError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("slack %s: %s (retry after %s)", e.Method, e.SlackError, e.RetryAfter)
	}
	return fmt.Sprintf("slack %s: %s", e.Method, e.SlackError)
}

// Package watch holds the canonical watch types shared by the store and
// wake packages.
//
// A watch is a registered condition: a room, a kind (edge, pattern,
// thread, or subscription), optional sender/pattern/thread filters, and a
// single-shot flag. Once is a pointer: nil means true (the wake.mjs
// default); persistent watches set it to false explicitly.
package watch

// Kind discriminates a watch's kind.
type Kind string

// Watch kinds.
const (
	// KindEdge watches for a reply to a specific message.
	KindEdge Kind = "edge"
	// KindPattern watches for a pattern in the text or an embed.
	KindPattern Kind = "pattern"
	// KindThread watches for a reply in a thread.
	KindThread Kind = "thread"
	// KindSubscription watches a thread the bot posted in, excluding the
	// bot's own posts.
	KindSubscription Kind = "subscription"
)

// Watch is a registered condition the wake policy evaluates.
type Watch struct {
	ID       string
	Owner    string
	RoomID   string
	Kind     Kind
	FromUser string
	Pattern  string
	ThreadID string
	ReplyTo  string
	Note     string
	// Once is nil = true (the wake.mjs default); persistent watches set
	// it to false explicitly.
	Once *bool
	// CreatedAt and ExpiresAt are unix seconds.
	CreatedAt int64
	ExpiresAt int64
}

package wake

import (
	"context"
	"regexp"
	"strings"
	"time"
)

// Kind discriminates the reason a wake fired.
type Kind string

const (
	// KindMention is a wake from an explicit mention of the bot.
	KindMention Kind = "mention"
	// KindReply is a wake from a reply to one of the bot's own messages.
	KindReply Kind = "reply"
	// KindWatch is a wake from a registered watch matching (or expiring).
	KindWatch Kind = "watch"
	// KindAmbient is a wake from the ambient gate.
	KindAmbient Kind = "ambient"
)

// Watch kinds.
const (
	// WatchKindEdge watches for a reply to a specific message.
	WatchKindEdge = "edge"
	// WatchKindPattern watches for a pattern in the text or an embed.
	WatchKindPattern = "pattern"
	// WatchKindThread watches for a reply in a thread.
	WatchKindThread = "thread"
	// WatchKindSubscription watches a thread the bot posted in, excluding
	// the bot's own posts.
	WatchKindSubscription = "subscription"
)

// From identifies the author of a message.
type From struct {
	UserID string
	Name   string
}

// Embed is the title/caption of an attachment. Pattern matching covers
// embeds because attachments ride as embeds, never in the message text.
type Embed struct {
	Title   string
	Caption string
}

// Msg is the inbound message record evaluated by the policy.
type Msg struct {
	RoomID   string
	From     From
	Text     string
	Mentions []string
	// ReplyTo is the message_id of reply_to, "" when absent.
	ReplyTo string
	Embeds  []Embed
}

// Self is the bot's own identity.
type Self struct {
	UserID  string
	Name    string
	Aliases []string
}

// Peer is one entry of the peers list.
type Peer struct {
	Name  string
	IsBot bool
}

// Peers maps user_id to a Peer.
type Peers map[string]Peer

// Watch is a registered condition the policy evaluates.
type Watch struct {
	ID       string
	RoomID   string
	Kind     string
	FromUser string
	Pattern  string
	ThreadID string
	ReplyTo  string
	Note     string
	Once     bool
}

// HistoryLookup resolves the author of a message from room history. ok is
// false when the message is not found.
type HistoryLookup func(ctx context.Context, messageID string) (fromUserID string, ok bool, err error)

// Classifier decides whether a message is directed at the bot.
type Classifier interface {
	IsDirected(ctx context.Context, msg Msg, self Self) (bool, error)
}

// Clock is the time source.
type Clock interface {
	Now() time.Time
}

// In carries everything Evaluate needs.
type In struct {
	Msg        Msg
	Self       Self
	Peers      Peers
	Watches    []Watch
	History    HistoryLookup
	Classifier Classifier
	Gate       *AmbientGate
	Clock      Clock
}

// Decision is the outcome of one evaluation.
type Decision struct {
	Wake           bool
	Kind           Kind
	Reason         string
	RemoveWatchIDs []string
}

// Evaluate runs the wake policy in order: mention, reply-to-my-message,
// watch match, bot firebreak, ambient gate. The first match wins.
func Evaluate(ctx context.Context, in In) Decision {
	if name, ok := mentioned(in.Self, in.Msg); ok {
		return Decision{Wake: true, Kind: KindMention, Reason: `mentioned "` + name + `"`}
	}
	if in.Msg.ReplyTo != "" && in.History != nil {
		if author, ok, err := in.History(ctx, in.Msg.ReplyTo); err == nil && ok && author == in.Self.UserID {
			return Decision{Wake: true, Kind: KindReply, Reason: "reply to my message"}
		}
	}
	if w, ok := matchWatch(in.Msg, in.Self, in.Watches); ok {
		d := Decision{Wake: true, Kind: KindWatch, Reason: w.Note}
		if w.Once {
			d.RemoveWatchIDs = []string{w.ID}
		}
		return d
	}
	if isBot(in.Peers, in.Msg.From.UserID) {
		return Decision{}
	}
	if in.Gate != nil && in.Clock != nil && in.Classifier != nil {
		now := in.Clock.Now()
		if in.Gate.Allow(now) {
			directed, err := in.Classifier.IsDirected(ctx, in.Msg, in.Self)
			if err != nil {
				// Fail-open: a false positive costs one cheap no-op wake;
				// a false negative silently drops a real message.
				directed = true
			}
			if directed {
				in.Gate.Record(now)
				return Decision{Wake: true, Kind: KindAmbient, Reason: "ambient"}
			}
		}
	}
	return Decision{}
}

// ExpiryDecision is the wake decision for a watch that expired without
// matching. The caller obtains due watches from the store and delivers
// these decisions.
func ExpiryDecision(w Watch) Decision {
	return Decision{Wake: true, Kind: KindWatch, Reason: "no match within ttl: " + w.Note}
}

// mentioned reports whether the message mentions the bot, and returns the
// matched name or alias for the reason.
func mentioned(self Self, msg Msg) (string, bool) {
	for _, m := range msg.Mentions {
		if m == self.UserID {
			return self.Name, true
		}
	}
	nameLower := strings.ToLower(self.Name)
	if nameLower != "" {
		lower := strings.ToLower(msg.Text)
		if strings.HasPrefix(lower, nameLower+":") || strings.HasPrefix(lower, nameLower+",") {
			return self.Name, true
		}
	}
	for _, n := range append([]string{self.Name}, self.Aliases...) {
		if containsWord(msg.Text, n) {
			return n, true
		}
	}
	return "", false
}

// containsWord reports whether word appears in text on a word boundary,
// case-insensitively. An uncompileable pattern is a non-match, not a
// panic.
func containsWord(text, word string) bool {
	if word == "" {
		return false
	}
	re, err := regexp.Compile(`(?i)\b` + regexp.QuoteMeta(word) + `\b`)
	if err != nil {
		return false
	}
	return re.MatchString(text)
}

// isBot reports whether the sender is a known bot in the peers list. An
// unknown sender is not a bot.
func isBot(peers Peers, userID string) bool {
	p, ok := peers[userID]
	return ok && p.IsBot
}

// matchWatch returns the first watch that matches the message.
func matchWatch(msg Msg, self Self, watches []Watch) (Watch, bool) {
	for _, w := range watches {
		if w.RoomID != "" && w.RoomID != msg.RoomID {
			continue
		}
		if w.FromUser != "" && w.FromUser != msg.From.UserID {
			continue
		}
		if w.Kind == WatchKindSubscription && msg.From.UserID == self.UserID {
			continue
		}
		if watchMatches(w, msg) {
			return w, true
		}
	}
	return Watch{}, false
}

// watchMatches reports whether the watch's kind-specific condition holds.
// Thread membership is resolved by a single reply_to hop: the message is a
// reply whose target is the thread root (v1).
func watchMatches(w Watch, msg Msg) bool {
	switch w.Kind {
	case WatchKindEdge:
		return msg.ReplyTo != "" && msg.ReplyTo == w.ReplyTo
	case WatchKindPattern:
		if patternMatches(w.Pattern, msg.Text) {
			return true
		}
		for _, e := range msg.Embeds {
			if patternMatches(w.Pattern, e.Title) || patternMatches(w.Pattern, e.Caption) {
				return true
			}
		}
		return false
	case WatchKindThread, WatchKindSubscription:
		return msg.ReplyTo != "" && msg.ReplyTo == w.ThreadID
	default:
		return false
	}
}

// patternMatches compiles the pattern (case-insensitive) and tests it
// against text. An uncompileable pattern is a non-match, not a panic.
func patternMatches(pattern, text string) bool {
	if pattern == "" {
		return false
	}
	re, err := regexp.Compile(`(?i)` + pattern)
	if err != nil {
		return false
	}
	return re.MatchString(text)
}

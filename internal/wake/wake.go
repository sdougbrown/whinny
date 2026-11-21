package wake

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/sdougbrown/whinny/internal/watch"
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

// WatchHit is one matching watch in a Decision, carrying the owner
// attribution the caller needs to route the wake.
type WatchHit struct {
	ID    string
	Owner string
	Note  string
	Once  bool
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
	Watches    []watch.Watch
	History    HistoryLookup
	Classifier Classifier
	Gate       *AmbientGate
	Clock      Clock
}

// Decision is the outcome of one evaluation.
type Decision struct {
	Wake   bool
	Kind   Kind
	Reason string
	// Watches is every watch that matched, on any fired path.
	Watches []WatchHit
	// RemoveWatchIDs is the id of every once-watch that matched; the
	// caller removes them.
	RemoveWatchIDs []string
}

// Evaluate runs the wake policy:
//
//   - mention: any sender that mentions the bot wakes; the evaluation
//     stops there.
//   - bot sender: only watches are evaluated (firebreak); a match wakes
//     with kind watch, otherwise the message is ignored.
//   - human sender: reply-to-my-message, then watch matches, then the
//     ambient gate (only when nothing above fired).
//
// Watch hits ride along on reply and watch wakes (the mention path
// short-circuits first, matching the reference). Kind is the
// highest-priority fired path (mention > reply > watch > ambient); Wake
// is true if any path fired; Reason comes from the primary path.
func Evaluate(ctx context.Context, in In) Decision {
	if name, ok := mentioned(in.Self, in.Msg); ok {
		return Decision{Wake: true, Kind: KindMention, Reason: `mentioned "` + name + `"`}
	}
	hits := matchWatches(in.Msg, in.Self, in.Watches)
	if isBot(in.Peers, in.Msg.From.UserID) {
		if len(hits) == 0 {
			return Decision{}
		}
		return watchDecision(hits)
	}
	replyFired := false
	if in.Msg.ReplyTo != "" && in.History != nil {
		if author, ok, err := in.History(ctx, in.Msg.ReplyTo); err == nil && ok && author == in.Self.UserID {
			replyFired = true
		}
	}
	if !replyFired && len(hits) == 0 && in.Gate != nil && in.Clock != nil && in.Classifier != nil {
		now := in.Clock.Now()
		if in.Gate.TryReserve(now) {
			directed, err := in.Classifier.IsDirected(ctx, in.Msg, in.Self)
			if err != nil {
				// Fail-open: a false positive costs one cheap no-op wake;
				// a false negative silently drops a real message.
				directed = true
			}
			if directed {
				return Decision{Wake: true, Kind: KindAmbient, Reason: "ambient"}
			}
			// A definitive no releases the reservation so the valve
			// state stays exact.
			in.Gate.Release(now)
		}
	}
	if replyFired {
		return Decision{
			Wake:           true,
			Kind:           KindReply,
			Reason:         "reply to my message",
			Watches:        hits,
			RemoveWatchIDs: onceIDs(hits),
		}
	}
	if len(hits) > 0 {
		return watchDecision(hits)
	}
	return Decision{}
}

// watchDecision builds the decision for a fired set of watch hits.
func watchDecision(hits []WatchHit) Decision {
	return Decision{
		Wake:           true,
		Kind:           KindWatch,
		Reason:         hits[0].Note,
		Watches:        hits,
		RemoveWatchIDs: onceIDs(hits),
	}
}

// onceIDs returns the ids of the once-hits, in hit order.
func onceIDs(hits []WatchHit) []string {
	var ids []string
	for _, h := range hits {
		if h.Once {
			ids = append(ids, h.ID)
		}
	}
	return ids
}

// ExpiryDecision is the wake decision for a watch that expired without
// matching. The caller obtains due watches from the store and delivers
// these decisions.
func ExpiryDecision(w watch.Watch) Decision {
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

// matchWatches returns every watch that matches the message, in list
// order. A watch with an empty RoomID never matches.
func matchWatches(msg Msg, self Self, watches []watch.Watch) []WatchHit {
	var hits []WatchHit
	for _, w := range watches {
		if w.RoomID == "" || w.RoomID != msg.RoomID {
			continue
		}
		if w.FromUser != "" && w.FromUser != msg.From.UserID {
			continue
		}
		if w.Kind == watch.KindSubscription && msg.From.UserID == self.UserID {
			continue
		}
		if watchMatches(w, msg) {
			once := w.Once == nil || *w.Once
			hits = append(hits, WatchHit{ID: w.ID, Owner: w.Owner, Note: w.Note, Once: once})
		}
	}
	return hits
}

// watchMatches reports whether the watch's kind-specific condition holds.
// Thread membership is resolved by a single reply_to hop: the message is a
// reply whose target is the thread root (v1). An unknown kind never
// matches.
func watchMatches(w watch.Watch, msg Msg) bool {
	switch w.Kind {
	case watch.KindEdge:
		return msg.ReplyTo != "" && msg.ReplyTo == w.ReplyTo
	case watch.KindPattern:
		if patternMatches(w.Pattern, msg.Text) {
			return true
		}
		for _, e := range msg.Embeds {
			if patternMatches(w.Pattern, e.Title) || patternMatches(w.Pattern, e.Caption) {
				return true
			}
		}
		return false
	case watch.KindThread, watch.KindSubscription:
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

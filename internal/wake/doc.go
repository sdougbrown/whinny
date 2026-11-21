// Package wake turns inbound room traffic into typed wake decisions.
//
// It is a pure package: no sqlite, no websocket. All I/O is behind
// interfaces (HistoryLookup, Classifier) and an injected Clock. The daemon
// owns the AmbientGate state and the watch list; this package only decides.
//
// The decision is first-match-wins over an ordered evaluation:
//
//  1. Mention — the bot's name (or an alias) appears on a word boundary,
//     as an @name/@alias token, as a leading "name:"/"name," prefix, or in
//     the message's mentions array. Any sender wakes, humans and bots alike.
//  2. Reply-to-my-message — the message replies to one of the bot's own
//     messages, resolved from room history, never in-memory state.
//  3. Watch match — a registered watch (edge, pattern, thread, or
//     subscription) matches. Once-watches are reported for removal.
//  4. Bot firebreak — a bot sender that matched nothing above is ignored;
//     bots wake the bot only via an explicit mention or a watch.
//  5. Ambient gate — a human sender that matched nothing above is passed to
//     the Classifier, bounded by the AmbientGate valves (min spacing, hourly
//     cap). The gate fails open on classifier error.
package wake

#!/usr/bin/env bash
# Prove whinny end-to-end against a real aprond:
#
#   1. build whinny, the e2e helper peer, and aprond
#   2. start aprond on an ephemeral port (temp data dir, email codes to
#      the server log)
#   3. enroll two identities through the email sign-in ceremony: the bot
#      "whinny-e2e" and the human "doug-e2e"
#   4. start the whinny daemon: resident mode, exec delivery into a sink
#      file, doug-e2e mapped as a human peer
#   5. the human mentions the bot in the seeded general room; assert a
#      kind=mention wake with the message text reaches the sink
#   5b. the bot sets a reaction through the CLI send path, the human
#       reacts to the bot's message; assert a kind=reaction wake
#   6. register a watch, the human matches it; assert a kind=watch wake
#   7. register a second watch that never matches; assert its
#      kind=watch-expired wake after the ttl
#   8. arm quiet, attempt a bot post; assert refused-quiet and that no
#      bot message reached the room's history
#
# Everything runs under a mktemp directory; a trap cleans up processes
# and files. Waits poll a condition with a deadline — never a fixed
# sleep. APRON_SERVER_DIR overrides where aprond lives (default
# ~/Code/apron-server-go).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APRON_DIR="${APRON_SERVER_DIR:-$HOME/Code/apron-server-go}"
WORK="$(mktemp -d)"
APROND_PID=""
DAEMON_PID=""
FAILED=0

cleanup() {
  [[ -n "$DAEMON_PID" ]] && kill "$DAEMON_PID" 2>/dev/null || true
  [[ -n "$APROND_PID" ]] && kill "$APROND_PID" 2>/dev/null || true
  # Reap the killed processes so their exit codes don't leak out of the
  # trap.
  [[ -n "$DAEMON_PID" ]] && wait "$DAEMON_PID" 2>/dev/null || true
  [[ -n "$APROND_PID" ]] && wait "$APROND_PID" 2>/dev/null || true
  if [[ "$FAILED" -eq 1 ]]; then
    printf 'e2e: artifacts kept at %s\n' "$WORK"
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

log()  { printf 'e2e: %s\n' "$*"; }
fail() { printf 'e2e: FAIL: %s\n' "$*" >&2; FAILED=1; exit 1; }

# Build with the toolchain each repo's go.mod asks for, using a cached
# one when the local go is older (GOTOOLCHAIN=local would refuse).
export GOTOOLCHAIN="${GOTOOLCHAIN:-auto}"

# wait_for <timeout-seconds> <description> <command...>: poll a command
# every 200ms until it succeeds or the deadline passes.
wait_for() {
  local timeout=$1 desc=$2; shift 2
  local deadline=$((SECONDS + timeout))
  until "$@"; do
    if (( SECONDS >= deadline )); then
      fail "timeout after ${timeout}s waiting for $desc"
    fi
    sleep 0.2
  done
}

# await_envelope <grep-pattern> <timeout-seconds> <description>: poll the
# sink file for an envelope matching the pattern, failing fast if the
# daemon process dies while waiting.
await_envelope() {
  local pattern=$1 timeout=$2 desc=$3
  local deadline=$((SECONDS + timeout))
  until grep -q -- "$pattern" "$SINK_FILE" 2>/dev/null; do
    if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      echo "--- daemon log ---" >&2; cat "$WORK/daemon.log" >&2
      fail "daemon exited while waiting for $desc"
    fi
    if (( SECONDS >= deadline )); then
      echo "--- daemon log ---" >&2; cat "$WORK/daemon.log" >&2
      echo "--- sink so far ---" >&2; cat "$SINK_FILE" 2>/dev/null >&2
      fail "timeout after ${timeout}s waiting for $desc"
    fi
    sleep 0.2
  done
  log "PASS: saw $desc"
}

# --- 1. build ---------------------------------------------------------------
log "building whinny and the e2e helper peer"
( cd "$ROOT" && go build -o "$WORK/whinny" ./cmd/whinny && go build -o "$WORK/aprontool" ./e2e/aprontool )
log "building aprond"
( cd "$APRON_DIR" && go build -o "$WORK/aprond" ./cmd/aprond )

# --- 2. start aprond --------------------------------------------------------
PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
URL="ws://127.0.0.1:$PORT/ws"
log "starting aprond on 127.0.0.1:$PORT"
"$WORK/aprond" --addr "127.0.0.1:$PORT" --store "sqlite:$WORK/aprond.db" \
  --email.enable --email.sender log >"$WORK/aprond.log" 2>&1 &
APROND_PID=$!
wait_for 15 "aprond /healthz" curl -sf "http://127.0.0.1:$PORT/healthz"
log "PASS: aprond healthy"

# --- 3. enroll the bot and the human ---------------------------------------
enroll() { # <email> <user-id> <name>; prints the session token
  local out
  out="$("$WORK/aprontool" enroll --url "$URL" --email "$1" --user-id "$2" --name "$3" --log "$WORK/aprond.log")"
  # Positive control: the helper's output must carry the user_id we
  # asked for, or the user_id greps below would pass vacuously.
  grep -q "\"user_id\":\"$2\"" <<<"$out" \
    || fail "enrollment output for $2 does not carry its user_id: $out"
  python3 -c 'import json,sys; print(json.loads(sys.argv[1])["token"])' "$out"
}
BOT_TOKEN="$(enroll whinny-e2e@apron.local whinny-e2e Whinny)"
HUMAN_TOKEN="$(enroll doug-e2e@apron.local doug-e2e Doug)"
[[ -n "$BOT_TOKEN" && -n "$HUMAN_TOKEN" ]] || fail "enrollment returned an empty token"
log "PASS: enrolled whinny-e2e (bot) and doug-e2e (human)"

# --- 4. start the whinny daemon --------------------------------------------
cat > "$WORK/daemon.json" <<EOF
{
  "agent": "whinny-e2e",
  "mode": "resident",
  "deliver": "exec",
  "exec_cmd": ["$ROOT/e2e/sink.sh"],
  "peers": { "doug-e2e": { "name": "Doug", "is_bot": false } },
  "self": { "user_id": "whinny-e2e", "name": "whinny-e2e" }
}
EOF
export WHINNY_URL="$URL" WHINNY_TOKEN="$BOT_TOKEN" WHINNY_DB="$WORK/whinny.db" \
  WHINNY_CONFIG="$WORK/daemon.json" SINK_FILE="$WORK/sink.out"
log "starting whinny daemon (resident, exec delivery)"
"$WORK/whinny" daemon >"$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
# Sync on the daemon's authenticated-connection log line: pushes reach
# the bot only from here on, so nothing may be posted before it.
wait_for 15 "daemon to connect" grep -q "daemon: connected" "$WORK/daemon.log"

# --- 5. mention wake --------------------------------------------------------
"$WORK/aprontool" say --url "$URL" --token "$HUMAN_TOKEN" --room general \
  --text "whinny-e2e can you look at the roster" --mention whinny-e2e >/dev/null
await_envelope "kind=mention" 30 "mention wake"
grep -q "from=doug-e2e" "$SINK_FILE" || fail "mention envelope missing from=doug-e2e"
grep -q "can you look at the roster" "$SINK_FILE" || fail "mention envelope missing the message text"
log "PASS: mention wake envelope carries the header and the roster text"

# --- 5b. reactions ----------------------------------------------------------
# The bot reacts to the human's mention through the CLI send path (the
# guard chain runs), then the human reacts to a bot message and the
# daemon delivers a kind=reaction wake.
HUMAN_MSG_ID="$(grep -o 'message_id=[^ ]*' "$SINK_FILE" | tail -1 | sed 's/^message_id=//;s/]$//')"
[[ -n "$HUMAN_MSG_ID" ]] || fail "could not extract the mention's message_id from the sink"
"$WORK/whinny" reactions "👍" --room general --message "$HUMAN_MSG_ID" >/dev/null \
  || fail "bot reactions command failed"
log "PASS: bot set 👍 on the human's message"
BOT_POST="$("$WORK/whinny" post --room general "roster review starting" --json 2>"$WORK/post.err")"   || { cat "$WORK/post.err" >&2; fail "bot post for the reaction target failed"; }
BOT_MSG_ID="$(printf '%s' "$BOT_POST" | python3 -c 'import json,sys; print(json.load(sys.stdin)["message_id"])')"
"$WORK/aprontool" react --url "$URL" --token "$HUMAN_TOKEN" --message "$BOT_MSG_ID" \
  --emoji "🎉" >/dev/null || fail "human react failed"
await_envelope "kind=reaction" 30 "reaction wake"
grep -q "🎉 from doug-e2e" "$SINK_FILE" || fail "reaction envelope missing the emoji and author"
log "PASS: reaction wake envelope carries the emoji and the author"

# --- 6. watch wake ----------------------------------------------------------
"$WORK/whinny" watch add --room general --pattern "deploy done" \
  --note "waiting for deploy" --ttl 30s >/dev/null
"$WORK/aprontool" say --url "$URL" --token "$HUMAN_TOKEN" --room general \
  --text "deploy done, all green" >/dev/null
await_envelope "kind=watch" 30 "watch wake"
grep -q 'note="waiting for deploy"' "$SINK_FILE" || fail "watch envelope missing its note"
grep -q "deploy done, all green" "$SINK_FILE" || fail "watch envelope missing the message text"
log "PASS: watch wake envelope carries the note and the matching text"

# --- 7. watch expiry --------------------------------------------------------
# The first watch fired (once) and was removed, so expiry needs a second
# watch that never matches. The expiry sweep runs every 60s, and the
# watch's ttl is 30s: allow 120s.
"$WORK/whinny" watch add --room general --pattern "no-match-e2e" \
  --note "waiting for nothing" --ttl 30s >/dev/null
await_envelope "kind=watch-expired" 120 "watch-expired wake"
grep -q 'note="waiting for nothing"' "$SINK_FILE" || fail "watch-expired envelope missing its note"
log "PASS: expired watch delivered a watch-expired wake"

# --- 8. quiet gates the send path -------------------------------------------
"$WORK/whinny" quiet --for 1h >/dev/null
if "$WORK/whinny" post --room general "hello from the bot" \
    >"$WORK/post.out" 2>"$WORK/post.err"; then
  fail "post succeeded while quiet was armed"
fi
grep -q "refused-quiet" "$WORK/post.out" "$WORK/post.err" \
  || fail "post refusal output does not mention refused-quiet"
log "PASS: quiet post refused with refused-quiet"
WHINNY_TOKEN="$HUMAN_TOKEN" "$WORK/whinny" history --room general --json \
  >"$WORK/history.json"
# The bot's pre-quiet messages (the roster reaction's author and its
# own post) are legitimately in history; quiet only gates new sends.
if grep -q '"text": "hello from the bot"' "$WORK/history.json"; then
  fail "history shows the quiet-gated post despite the refusal"
fi
log "PASS: history (as the human) shows no message from the bot"

log "PASS: whinny e2e complete (server: aprond v9, mode resident, deliver exec)"

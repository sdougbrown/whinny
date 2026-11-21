#!/usr/bin/env bash
# Sink delivery target for whinny e2e: append the wake envelope on stdin
# to the file $SINK_FILE names. The daemon spawns this per wake with the
# envelope on stdin (the exec backend), inheriting the environment run.sh
# exported.
set -euo pipefail
: "${SINK_FILE:?SINK_FILE must be set}"
cat >> "$SINK_FILE"

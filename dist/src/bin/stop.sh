#!/bin/bash
#
# Copyright The Dongting Project
#
# The Dongting Project licenses this file to you under the Apache License,
# version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at:
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$BASE_DIR/data"
PID_FILE="$DATA_DIR/dongting.pid"

if [ ! -f "$PID_FILE" ]; then
  echo "No PID file $PID_FILE, dongting may not be running."
  exit 0
fi

PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
if [ -z "$PID" ] || [ "$PID" -le 0 ] 2>/dev/null; then
  echo "Invalid PID in $PID_FILE, removing it."
  rm -f "$PID_FILE" 2>/dev/null || true
  exit 0
fi

if ! kill -0 "$PID" 2>/dev/null; then
  echo "No process with PID $PID, removing stale PID file $PID_FILE."
  rm -f "$PID_FILE" 2>/dev/null || true
  exit 0
fi

TERM_WAIT=${TERM_WAIT_SECONDS:-60}
FORCE=${DONGTING_FORCE_KILL:-1}

echo "Stopping dongting (PID $PID) with SIGTERM..."
kill "$PID" 2>/dev/null || {
  echo "Failed to send SIGTERM to PID $PID" >&2
  exit 1
}

end=$((SECONDS + TERM_WAIT))
while kill -0 "$PID" 2>/dev/null; do
  if [ $SECONDS -ge $end ]; then
    echo "Process $PID did not exit within $TERM_WAIT seconds."
    if [ "$FORCE" = "1" ]; then
      echo "Sending SIGKILL to $PID..."
      kill -9 "$PID" 2>/dev/null || {
        echo "Failed to send SIGKILL to PID $PID" >&2
        exit 1
      }
      break
    else
      echo "Force kill disabled (DONGTING_FORCE_KILL!=1)." >&2
      exit 1
    fi
  fi
  sleep 1
done

if kill -0 "$PID" 2>/dev/null; then
  echo "Process $PID still running after force kill attempt." >&2
  exit 1
fi

rm -f "$PID_FILE" 2>/dev/null || true

echo "dongting stopped (PID $PID)."

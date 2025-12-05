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

# allow advanced users to skip cmdline verification (not recommended)
# set DONGTING_SKIP_CMDLINE_CHECK=1 to skip verification
SKIP_VERIFY=${DONGTING_SKIP_CMDLINE_CHECK:-0}

# normalize base/data dir for comparison
if command -v realpath >/dev/null 2>&1; then
  EXPECT_BASE="$(realpath "$BASE_DIR" 2>/dev/null || echo "$BASE_DIR")"
  EXPECT_DATA="$(realpath "$DATA_DIR" 2>/dev/null || echo "$DATA_DIR")"
else
  EXPECT_BASE="$(cd "$BASE_DIR" 2>/dev/null && pwd)"
  EXPECT_DATA="$(cd "$DATA_DIR" 2>/dev/null && pwd)"
fi

get_cmdline() {
  local pid="$1"
  local cmdline=""
  if [ -r "/proc/$pid/cmdline" ]; then
    cmdline=$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null | sed 's/[[:space:]]\+/ /g')
  fi
  if [ -z "$cmdline" ]; then
    cmdline=$(ps -p "$pid" -o args= 2>/dev/null | sed 's/[[:space:]]\+/ /g')
  fi
  echo "$cmdline"
}

verify_process() {
  local pid="$1"

  if [ "$SKIP_VERIFY" = "1" ]; then
    # skip verification when explicitly requested
    return 0
  fi

  local cmdline
  cmdline="$(get_cmdline "$pid")"
  if [ -z "$cmdline" ]; then
    echo "Cannot read command line for PID $pid; refusing to stop because ownership cannot be verified." >&2
    return 1
  fi

  # require dongting ops main marker and matching base/data dir
  case "$cmdline" in
    *"dongting.ops/com.github.dtprj.dongting.boot.Bootstrap"*) ;;
    *)
      echo "PID $pid command line does not look like a dongting server process: $cmdline" >&2
      return 1
      ;;
  esac

  # check data dir marker as passed by start-dongting.sh
  case "$cmdline" in
    *"-DDATA_DIR=$EXPECT_DATA"*|*"-DDATA_DIR=$DATA_DIR"*) data_ok=1 ;;
    *) data_ok=0 ;;
  esac

  if [ "$data_ok" -ne 1 ]; then
    echo "PID $pid command line does not contain expected DATA_DIR ($EXPECT_DATA): $cmdline" >&2
    return 1
  fi

  return 0
}

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

# verify that this PID really belongs to current dongting instance
if ! verify_process "$PID"; then
  echo "Refusing to stop PID $PID because it does not match dongting under BASE_DIR=$BASE_DIR DATA_DIR=$DATA_DIR" >&2
  exit 1
fi

TERM_WAIT=${TERM_WAIT_SECONDS:-60}
FORCE=${DONGTING_FORCE_KILL:-1}

echo "Stopping dongting (PID $PID) with SIGTERM..."
kill -s TERM "$PID" 2>/dev/null || {
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

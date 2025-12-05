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

# Resolve the script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

CONF_DIR="$BASE_DIR/conf"
LIB_DIR="$BASE_DIR/lib"
LOG_DIR="$BASE_DIR/logs"
DATA_DIR="$BASE_DIR/data"
PID_FILE="$DATA_DIR/dongting.pid"

# JVM options
JAVA_OPTS="-Xms4g -Xmx4g -XX:MaxDirectMemorySize=2g"

# Check if JAVA_HOME is set
if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA="java"
fi

mkdir -p "$DATA_DIR" || {
  echo "Failed to create data dir: $DATA_DIR" >&2
  exit 1
}

# Check existing PID file
if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
  if [ -n "$OLD_PID" ] && [ "$OLD_PID" -gt 0 ] 2>/dev/null; then
    if kill -0 "$OLD_PID" 2>/dev/null; then
      echo "dongting already running with PID $OLD_PID (PID file: $PID_FILE)" >&2
      exit 1
    else
      echo "Removing stale PID file $PID_FILE (no process $OLD_PID)" >&2
      rm -f "$PID_FILE" || {
        echo "Failed to remove stale PID file $PID_FILE" >&2
        exit 1
      }
    fi
  else
    echo "Invalid PID file $PID_FILE, removing" >&2
    rm -f "$PID_FILE" || {
      echo "Failed to remove invalid PID file $PID_FILE" >&2
      exit 1
    }
  fi
fi

# Ensure LOG_DIR exists
mkdir -p "$LOG_DIR" || {
  echo "Failed to create log dir: $LOG_DIR" >&2
  exit 1
}

# Start the application using module path in background and record PID
nohup "$JAVA" $JAVA_OPTS \
    -DDATA_DIR="$DATA_DIR" \
    -DLOG_DIR="$LOG_DIR" \
    -Dlogback.configurationFile="$CONF_DIR/logback.xml" \
    --module-path "$LIB_DIR" \
    --add-exports java.base/jdk.internal.misc=dongting.client \
    --add-modules org.slf4j,ch.qos.logback.classic \
    --add-reads dongting.client=org.slf4j \
    --add-reads dongting.client=ch.qos.logback.classic \
    -m dongting.ops/com.github.dtprj.dongting.boot.Bootstrap \
    -c "$CONF_DIR/config.properties" \
    -s "$CONF_DIR/servers.properties" \
    "$@" > "$LOG_DIR/start.log" 2>&1 &
NEW_PID=$!
if [ -z "$NEW_PID" ] || [ "$NEW_PID" -le 0 ] 2>/dev/null; then
  echo "Failed to start dongting (no PID captured)" >&2
  exit 1
fi

# Verify the process actually started and is running
sleep 1
if ! kill -0 "$NEW_PID" 2>/dev/null; then
  echo "Failed to start dongting: process $NEW_PID exited immediately after start" >&2
  echo "Check $LOG_DIR/start.log for details" >&2
  exit 1
fi

# Verify it's the expected Java process
if ! ps -p "$NEW_PID" -o args= 2>/dev/null | grep -q "dongting.ops/com.github.dtprj.dongting.boot.Bootstrap"; then
  echo "Failed to start dongting: PID $NEW_PID is not a dongting process" >&2
  kill "$NEW_PID" 2>/dev/null || true
  exit 1
fi

echo "$NEW_PID" >"$PID_FILE" || {
  echo "Failed to write PID file $PID_FILE" >&2
  kill "$NEW_PID" 2>/dev/null || true
  exit 1
}

echo "dongting started with PID $NEW_PID (PID file: $PID_FILE)"

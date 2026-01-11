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

# JVM options, since the heap size is only 4g, zgc is not needed.
JAVA_OPTS="-Xms4g -Xmx4g -XX:MaxDirectMemorySize=2g -XX:+UseG1GC -XX:MaxGCPauseMillis=5 -XX:G1HeapRegionSize=2m -XX:+ParallelRefProcEnabled -XX:InitiatingHeapOccupancyPercent=30"

# Check if JAVA_HOME is set
if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA="java"
fi

# Ensure LOG_DIR exists for logback output
mkdir -p "$LOG_DIR" || {
  echo "Failed to create log dir: $LOG_DIR" >&2
  exit 1
}

# Launch DtBenchmark in foreground so stdout/stderr reach the console
"$JAVA" $JAVA_OPTS \
    -DLOG_DIR="$LOG_DIR" \
    -Dlogback.configurationFile="$CONF_DIR/logback-benchmark.xml" \
    --module-path "$LIB_DIR" \
    --add-exports java.base/jdk.internal.misc=dongting.client \
    --add-modules org.slf4j,ch.qos.logback.classic \
    --add-reads dongting.client=org.slf4j \
    --add-reads dongting.client=ch.qos.logback.classic \
    -m dongting.dist/com.github.dtprj.dongting.dist.DtBenchmark \
    -s "$CONF_DIR/client.properties" \
    "$@"


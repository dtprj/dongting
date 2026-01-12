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
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BASE_DIR = Split-Path -Parent $ScriptDir

$CONF_DIR = Join-Path $BASE_DIR "conf"
$LIB_DIR = Join-Path $BASE_DIR "lib"
$LOG_DIR = Join-Path $BASE_DIR "logs"
$DATA_DIR = Join-Path $BASE_DIR "data"
$PidFile = Join-Path $DATA_DIR "dongting.pid"

# JVM options, since the heap size is only 4g, zgc is not needed.
$JavaOpts = @("-Xms4g", "-Xmx4g", "-XX:MaxDirectMemorySize=2g", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=5", "-XX:G1HeapRegionSize=2m", "-XX:+ParallelRefProcEnabled", "-XX:InitiatingHeapOccupancyPercent=30")

# Check if JAVA_HOME is set
if ($env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME "bin\java.exe"))) {
    $Java = Join-Path $env:JAVA_HOME "bin\java.exe"
} else {
    $Java = "java"
}

if (-not (Test-Path $DATA_DIR)) {
    New-Item -ItemType Directory -Path $DATA_DIR -Force | Out-Null
}

if (-not (Test-Path $LOG_DIR)) {
    New-Item -ItemType Directory -Path $LOG_DIR -Force | Out-Null
}

# Check existing PID file
if (Test-Path $PidFile) {
    $pidText = Get-Content -Path $PidFile -ErrorAction SilentlyContinue
    $oldPid = $null
    if ($pidText -and [int]::TryParse($pidText.Trim(), [ref]$oldPid) -and $oldPid -gt 0) {
        $proc = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
        if ($proc) {
            Write-Error "dongting already running with PID $oldPid (PID file: $PidFile)"
            exit 1
        } else {
            Write-Warning "Removing stale PID file $PidFile (no process $oldPid)"
            Remove-Item -Path $PidFile -Force -ErrorAction SilentlyContinue
        }
    } else {
        Write-Warning "Invalid PID file $PidFile, removing"
        Remove-Item -Path $PidFile -Force -ErrorAction SilentlyContinue
    }
}

# Build arguments
$Arguments = $JavaOpts + @(
    "-DDATA_DIR=$DATA_DIR",
    "-DLOG_DIR=$LOG_DIR",
    "-Dlogback.configurationFile=$CONF_DIR\logback-server.xml",
    "--module-path", $LIB_DIR,
    "--add-exports", "java.base/jdk.internal.misc=dongting.client",
    "-m", "dongting.dist/com.github.dtprj.dongting.dist.Bootstrap",
    "-c", (Join-Path $CONF_DIR "config.properties"),
    "-s", (Join-Path $CONF_DIR "servers.properties")
) + $args

# Start the application and record PID
$process = Start-Process -FilePath $Java -ArgumentList $Arguments -PassThru -NoNewWindow -RedirectStandardOutput (Join-Path $LOG_DIR "start.log") -RedirectStandardError (Join-Path $LOG_DIR "start_error.log")
if (-not $process -or $process.Id -le 0) {
    Write-Error "Failed to start dongting (no PID captured)"
    exit 1
}

# Verify the process actually started and is running
Start-Sleep -Milliseconds 1500
$runningProc = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
if (-not $runningProc) {
    Write-Error "Failed to start dongting: process $($process.Id) exited immediately after start"
    Write-Error "Check $(Join-Path $LOG_DIR 'start.log') and $(Join-Path $LOG_DIR 'start_error.log') for details"
    exit 1
}

$process.Id | Out-File -FilePath $PidFile -Encoding ascii -Force
Write-Output "dongting started with PID $($process.Id) (PID file: $PidFile)"

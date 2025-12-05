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

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BASE_DIR = Split-Path -Parent $ScriptDir
$DATA_DIR = Join-Path $BASE_DIR "data"
$PidFile = Join-Path $DATA_DIR "dongting.pid"

# allow advanced users to skip cmdline verification (not recommended)
$verifyCmdline = $true
if ($env:DONGTING_SKIP_CMDLINE_CHECK -and $env:DONGTING_SKIP_CMDLINE_CHECK -ne "1") {
    $verifyCmdline = $false
}

function Get-DongtingProcessCommandLine {
    param(
        [int]$Pid
    )
    try {
        $procInfo = Get-CimInstance Win32_Process -Filter "ProcessId=$Pid" -ErrorAction Stop
        return $procInfo.CommandLine
    } catch {
        try {
            $procInfo = Get-WmiObject Win32_Process -Filter "ProcessId=$Pid" -ErrorAction Stop
            return $procInfo.CommandLine
        } catch {
            Write-Warning "Failed to read command line for PID $Pid: $_"
            return $null
        }
    }
}

function Test-DongtingProcess {
    param(
        [int]$Pid,
        [string]$BaseDir,
        [string]$DataDir,
        [bool]$VerifyCmdline
    )

    if (-not $VerifyCmdline) {
        return $true
    }

    $expectedBase = [System.IO.Path]::GetFullPath($BaseDir)
    $expectedData = [System.IO.Path]::GetFullPath($DataDir)

    $cmd = Get-DongtingProcessCommandLine -Pid $Pid
    if (-not $cmd) {
        Write-Warning "Cannot read command line for PID $Pid; refusing to stop because ownership cannot be verified."
        return $false
    }

    $cmdLower = $cmd.ToLowerInvariant()
    $marker = "dongting.ops/com.github.dtprj.dongting.boot.bootstrap"
    if (-not $cmdLower.Contains($marker)) {
        Write-Warning "PID $Pid command line does not look like a dongting server process: $cmd"
        return $false
    }

    $dataToken1 = "-DDATA_DIR=$expectedData"
    $dataToken2 = "-DDATA_DIR=$DataDir"

    if (-not ($cmd.Contains($dataToken1) -or $cmd.Contains($dataToken2))) {
        Write-Warning "PID $Pid command line does not contain expected DATA_DIR ($expectedData): $cmd"
        return $false
    }

    return $true
}

if (-not (Test-Path $PidFile)) {
    Write-Output "No PID file $PidFile, dongting may not be running."
    exit 0
}

$pidText = Get-Content -Path $PidFile -ErrorAction SilentlyContinue
if (-not $pidText) {
    Write-Warning "Invalid PID in $PidFile, removing it."
    Remove-Item -Path $PidFile -Force -ErrorAction SilentlyContinue
    exit 0
}

$targetPid = 0
if (-not [int]::TryParse($pidText.Trim(), [ref]$targetPid) -or $targetPid -le 0) {
    Write-Warning "Invalid PID in $PidFile, removing it."
    Remove-Item -Path $PidFile -Force -ErrorAction SilentlyContinue
    exit 0
}

$proc = Get-Process -Id $targetPid -ErrorAction SilentlyContinue
if (-not $proc) {
    Write-Output "No process with PID $targetPid, removing stale PID file $PidFile."
    Remove-Item -Path $PidFile -Force -ErrorAction SilentlyContinue
    exit 0
}

if (-not (Test-DongtingProcess -Pid $targetPid -BaseDir $BASE_DIR -DataDir $DATA_DIR -VerifyCmdline:$verifyCmdline)) {
    Write-Error "Refusing to stop PID $targetPid because it does not match dongting under BASE_DIR=$BASE_DIR DATA_DIR=$DATA_DIR"
    exit 1
}

$termWaitSeconds = 60
if ($env:TERM_WAIT_SECONDS) {
    [int]::TryParse($env:TERM_WAIT_SECONDS, [ref]$termWaitSeconds) | Out-Null
}
$forceKill = $true
if ($env:DONGTING_FORCE_KILL -and $env:DONGTING_FORCE_KILL -ne "1") {
    $forceKill = $false
}

Write-Output "Stopping dongting (PID $targetPid)..."
try {
    Stop-Process -Id $targetPid -ErrorAction SilentlyContinue
} catch {
    Write-Error "Failed to send termination signal to PID $targetPid: $_"
    exit 1
}

$deadline = (Get-Date).AddSeconds($termWaitSeconds)
while (Get-Process -Id $targetPid -ErrorAction SilentlyContinue) {
    if ((Get-Date) -ge $deadline) {
        Write-Warning "Process $targetPid did not exit within $termWaitSeconds seconds."
        if ($forceKill) {
            Write-Warning "Forcing termination of PID $targetPid..."
            try {
                Stop-Process -Id $targetPid -Force -ErrorAction SilentlyContinue
            } catch {
                Write-Error "Failed to force terminate PID $targetPid: $_"
                exit 1
            }
        } else {
            Write-Error "Force kill disabled (DONGTING_FORCE_KILL!=1)."
            exit 1
        }
        break
    }
    Start-Sleep -Seconds 1
}

if (Get-Process -Id $targetPid -ErrorAction SilentlyContinue) {
    Write-Error "Process $targetPid still running after stop attempts."
    exit 1
}

Remove-Item -Path $PidFile -Force -ErrorAction SilentlyContinue
Write-Output "dongting stopped (PID $targetPid)."

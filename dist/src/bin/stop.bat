@echo off
rem
rem Copyright The Dongting Project
rem
rem The Dongting Project licenses this file to you under the Apache License,
rem version 2.0 (the "License"); you may not use this file except in compliance
rem with the License. You may obtain a copy of the License at:
rem
rem   https://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
rem WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
rem License for the specific language governing permissions and limitations
rem under the License.
rem

setlocal

set "SCRIPT_DIR=%~dp0"
set "BASE_DIR=%SCRIPT_DIR%.."
set "DATA_DIR=%BASE_DIR%\data"
set "PID_FILE=%DATA_DIR%\dongting.pid"

rem allow advanced users to skip cmdline verification (not recommended)
rem set DONGTING_SKIP_CMDLINE_CHECK=1 to skip verification
if "%DONGTING_SKIP_CMDLINE_CHECK%"=="" (
    set "DONGTING_SKIP_CMDLINE_CHECK=0"
)

if not exist "%PID_FILE%" (
    echo No PID file %PID_FILE%, dongting may not be running.
    exit /b 0
)

set "PID="
set /p PID=<"%PID_FILE%"
for /f "tokens=*" %%i in ("%PID%") do set "PID=%%i"

echo %PID%| findstr /R "^[0-9][0-9]*$" >nul 2>&1
if errorlevel 1 (
    echo Invalid PID in %PID_FILE%, removing it.
    del /f /q "%PID_FILE%" >nul 2>&1
    exit /b 0
)

rem check process existence first
tasklist /FI "PID eq %PID%" | find "%PID%" >nul 2>&1
if errorlevel 1 (
    echo No process with PID %PID%, removing stale PID file %PID_FILE%.
    del /f /q "%PID_FILE%" >nul 2>&1
    exit /b 0
)

rem verify command line belongs to dongting under current DATA_DIR
if "%DONGTING_SKIP_CMDLINE_CHECK%"=="1" goto :skip_verify

powershell -NoProfile -Command "^
  param(^$pid,^$dataDir)^; ^
  try { ^
    ^$p = Get-CimInstance Win32_Process -Filter \"ProcessId=^$pid\" -ErrorAction Stop ^
  } catch { ^
    try { ^
      ^$p = Get-WmiObject Win32_Process -Filter \"ProcessId=^$pid\" -ErrorAction Stop ^
    } catch { ^
      Write-Error \"Failed to read command line for PID ^$pid: ^$_\"; exit 2 ^
    } ^
  } ^
  if (-not ^$p -or -not ^$p.CommandLine) { ^
    Write-Error \"Cannot read command line for PID ^$pid; refusing to stop because ownership cannot be verified.\"; exit 2 ^
  } ^
  ^$cmd = ^$p.CommandLine ^
  ^$cmdLower = ^$cmd.ToLowerInvariant() ^
  ^$marker = 'dongting.ops/com.github.dtprj.dongting.boot.bootstrap' ^
  if (-not ^$cmdLower.Contains(^$marker)) { ^
    Write-Error \"PID ^$pid command line does not look like a dongting server process: ^$cmd\"; exit 2 ^
  } ^
  ^$expectedData = [System.IO.Path]::GetFullPath(^$dataDir) ^
  ^$dataMatch = ( ^
    ^$cmd.Contains('-DDATA_DIR=' + ^$expectedData) -or ^
    ^$cmd.Contains('-DDATA_DIR="' + ^$expectedData + '"') -or ^
    ^$cmd.Contains('-DDATA_DIR=' + ^$dataDir) -or ^
    ^$cmd.Contains('-DDATA_DIR="' + ^$dataDir + '"') ^
  ) ^
  if (-not ^$dataMatch) { ^
    Write-Error \"PID ^$pid command line does not contain expected DATA_DIR (^$expectedData): ^$cmd\"; exit 2 ^
  } ^
" -- %PID% "%DATA_DIR%"

if errorlevel 2 (
    echo Refusing to stop PID %PID% because it does not match dongting under DATA_DIR=%DATA_DIR%.>&2
    exit /b 1
)

:skip_verify

if "%TERM_WAIT_SECONDS%"=="" set "TERM_WAIT_SECONDS=60"

if "%DONGTING_FORCE_KILL%"=="" set "DONGTING_FORCE_KILL=1"

echo Stopping dongting (PID %PID%)...

taskkill /PID %PID% /T >nul 2>&1
if errorlevel 1 (
    echo Failed to send terminate to PID %PID%.>&2
    exit /b 1
)

set /a elapsed=0
:wait_loop
    tasklist /FI "PID eq %PID%" | find "%PID%" >nul 2>&1
    if errorlevel 1 goto done
    if %elapsed% GEQ %TERM_WAIT_SECONDS% goto timeout
    timeout /t 1 /nobreak >nul
    set /a elapsed+=1
    goto wait_loop

:timeout
    echo Process %PID% did not exit within %TERM_WAIT_SECONDS% seconds.
    if "%DONGTING_FORCE_KILL%"=="1" (
        echo Forcing termination of PID %PID%...
        taskkill /PID %PID% /T /F >nul 2>&1
    ) else (
        echo Force kill disabled (DONGTING_FORCE_KILL!=1).>&2
        exit /b 1
    )

:done

tasklist /FI "PID eq %PID%" | find "%PID%" >nul 2>&1
if not errorlevel 1 (
    echo Process %PID% still running after stop attempts.>&2
    exit /b 1
)

del /f /q "%PID_FILE%" >nul 2>&1

echo dongting stopped (PID %PID%).

endlocal
exit /b 0

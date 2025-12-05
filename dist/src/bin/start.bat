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

rem Resolve the script directory
set "SCRIPT_DIR=%~dp0"
set "BASE_DIR=%SCRIPT_DIR%.."

set "CONF_DIR=%BASE_DIR%\conf"
set "LIB_DIR=%BASE_DIR%\lib"
set "LOG_DIR=%BASE_DIR%\logs"
set "DATA_DIR=%BASE_DIR%\data"
set "PID_FILE=%DATA_DIR%\dongting.pid"

set "JAVA_OPTS=-Xms4g -Xmx4g -XX:MaxDirectMemorySize=2g"

rem Check if JAVA_HOME is set
if defined JAVA_HOME (
    set "JAVA=%JAVA_HOME%\bin\java.exe"
) else (
    set "JAVA=java"
)

if not exist "%DATA_DIR%" (
    mkdir "%DATA_DIR%" 2>nul
)

if not exist "%LOG_DIR%" (
    mkdir "%LOG_DIR%" 2>nul
)

rem Check existing PID file
if exist "%PID_FILE%" (
    set "OLD_PID="
    set /p OLD_PID=<"%PID_FILE%"
    for /f "tokens=*" %%i in ("%OLD_PID%") do set "OLD_PID=%%i"
    echo %OLD_PID%| findstr /R "^[0-9][0-9]*$" >nul 2>&1
    if not errorlevel 1 (
        tasklist /FI "PID eq %OLD_PID%" | find "%OLD_PID%" >nul 2>&1
        if not errorlevel 1 (
            echo dongting already running with PID %OLD_PID% (PID file: %PID_FILE%)>&2
            exit /b 1
        ) else (
            echo Removing stale PID file %PID_FILE% (no process %OLD_PID%)
            del /f /q "%PID_FILE%" >nul 2>&1
        )
    ) else (
        echo Invalid PID file %PID_FILE%, removing
        del /f /q "%PID_FILE%" >nul 2>&1
    )
)

rem Start the application using PowerShell to properly capture PID and redirect output
powershell -NoProfile -Command "^
  param(^$java, ^$javaOpts, ^$dataDir, ^$logDir, ^$confDir, ^$libDir, ^$pidFile, ^$extraArgs)^; ^
  ^$logFile = Join-Path ^$logDir 'start.log'^; ^
  ^$errFile = Join-Path ^$logDir 'start_error.log'^; ^
  ^$arguments = @(^
    '-DDATA_DIR=' + ^$dataDir, ^
    '-DLOG_DIR=' + ^$logDir, ^
    '-Dlogback.configurationFile=' + (Join-Path ^$confDir 'logback.xml'), ^
    '--module-path', ^$libDir, ^
    '--add-exports', 'java.base/jdk.internal.misc=dongting.client', ^
    '--add-modules', 'org.slf4j,ch.qos.logback.classic', ^
    '--add-reads', 'dongting.client=org.slf4j', ^
    '--add-reads', 'dongting.client=ch.qos.logback.classic', ^
    '-m', 'dongting.ops/com.github.dtprj.dongting.boot.Bootstrap', ^
    '-c', (Join-Path ^$confDir 'config.properties'), ^
    '-s', (Join-Path ^$confDir 'servers.properties')^
  )^; ^
  if (^$javaOpts) { ^$arguments = ^$javaOpts.Split(' ') + ^$arguments }^; ^
  if (^$extraArgs) { ^$arguments += ^$extraArgs.Split(' ') }^; ^
  try { ^
    ^$process = Start-Process -FilePath ^$java -ArgumentList ^$arguments -PassThru -NoNewWindow -RedirectStandardOutput ^$logFile -RedirectStandardError ^$errFile^; ^
    if (-not ^$process -or ^$process.Id -le 0) {^
        Write-Error 'Failed to start dongting (no PID captured)'^; exit 1 ^
    }^; ^
    Start-Sleep -Milliseconds 1500^; ^
    ^$runningProc = Get-Process -Id ^$process.Id -ErrorAction SilentlyContinue^; ^
    if (-not ^$runningProc) {^
        Write-Error \"Failed to start dongting: process ^$(^$process.Id) exited immediately. Check ^$logFile and ^$errFile\"^; exit 1 ^
    }^; ^
    ^$process.Id ^| Out-File -FilePath ^$pidFile -Encoding ascii -Force^; ^
    Write-Output \"dongting started with PID ^$(^$process.Id) (PID file: ^$pidFile)\"^; ^
    exit 0 ^
  } catch { ^
    Write-Error \"Failed to start dongting: ^$_\"^; exit 1 ^
  } ^
" -- "%JAVA%" "%JAVA_OPTS%" "%DATA_DIR%" "%LOG_DIR%" "%CONF_DIR%" "%LIB_DIR%" "%PID_FILE%" "%*"

if errorlevel 1 (
    exit /b 1
)

endlocal

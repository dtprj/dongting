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

rem Start the application using module path in a new window and record PID via PowerShell
start "dongting" /b "%JAVA%" %JAVA_OPTS% ^
    -DDATA_DIR="%DATA_DIR%" ^
    -DLOG_DIR="%LOG_DIR%" ^
    -Dlogback.configurationFile="%CONF_DIR%\logback.xml" ^
    --module-path "%LIB_DIR%" ^
    --add-exports java.base/jdk.internal.misc=dongting.client ^
    --add-modules org.slf4j,ch.qos.logback.classic ^
    --add-reads dongting.client=org.slf4j ^
    --add-reads dongting.client=ch.qos.logback.classic ^
    -m dongting.ops/com.github.dtprj.dongting.boot.Bootstrap ^
    -c "%CONF_DIR%\config.properties" ^
    -s "%CONF_DIR%\servers.properties" ^
    %*

rem Use PowerShell helper to find the just-started process and write its PID file
powershell -NoProfile -Command "
$baseDir = [IO.Path]::GetFullPath('%BASE_DIR%');
$dataDir = Join-Path $baseDir 'data';
$pidFile = Join-Path $dataDir 'dongting.pid';
$procs = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*dongting.ops/com.github.dtprj.dongting.boot.Bootstrap*' -and $_.CommandLine -like '*-DDATA_DIR="%DATA_DIR%"*' };
$proc = $procs | Sort-Object CreationDate -Descending | Select-Object -First 1;
if ($proc -and $proc.ProcessId -gt 0) { $proc.ProcessId | Out-File -FilePath $pidFile -Encoding ascii -Force; Write-Output "dongting started with PID $($proc.ProcessId) (PID file: $pidFile)" } else { Write-Error 'Failed to capture dongting PID'; exit 1 }
"

endlocal

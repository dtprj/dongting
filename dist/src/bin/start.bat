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

rem JVM options
set "JAVA_OPTS=-Xms4g -Xmx4g -XX:MaxDirectMemorySize=2g"

rem Check if JAVA_HOME is set
if defined JAVA_HOME (
    set "JAVA=%JAVA_HOME%\bin\java.exe"
) else (
    set "JAVA=java"
)

rem Start the application using module path
"%JAVA%" %JAVA_OPTS% ^
    -DDATA_DIR="%DATA_DIR%" ^
    -DLOG_DIR="%LOG_DIR%" ^
    -Dlogback.configurationFile="%CONF_DIR%\logback.xml" ^
    --module-path "%LIB_DIR%" ^
    --add-exports java.base/jdk.internal.misc=dongting.client ^
    -m dongting.ops/com.github.dtprj.dongting.boot.Bootstrap ^
    -c "%CONF_DIR%\config.properties" ^
    -s "%CONF_DIR%\servers.properties" ^
    %*

endlocal

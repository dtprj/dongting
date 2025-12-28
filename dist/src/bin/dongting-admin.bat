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

setlocal ENABLEDELAYEDEXPANSION

set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

set "PS_SCRIPT=%SCRIPT_DIR%\dongting-admin.ps1"

set "POWERSHELL_EXE="
where pwsh >nul 2>&1
if not errorlevel 1 set "POWERSHELL_EXE=pwsh"

if "%POWERSHELL_EXE%"=="" where powershell >nul 2>&1
if "%POWERSHELL_EXE%"=="" if not errorlevel 1 set "POWERSHELL_EXE=powershell"

if "%POWERSHELL_EXE%"=="" goto :no_ps

goto :run_ps

:no_ps
echo Failed to find PowerShell (pwsh or powershell) in PATH.>&2
endlocal & exit /b 1

:run_ps
"%POWERSHELL_EXE%" -NoProfile -ExecutionPolicy Bypass -File "%PS_SCRIPT%" %*
set "EXIT_CODE=%ERRORLEVEL%"

endlocal & exit /b %EXIT_CODE%


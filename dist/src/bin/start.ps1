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
$BaseDir = Split-Path -Parent $ScriptDir

$ConfDir = Join-Path $BaseDir "conf"
$LibDir = Join-Path $BaseDir "lib"

# JVM options
$JavaOpts = @("-Xms4g", "-Xmx4g", "-XX:MaxDirectMemorySize=2g")

# Check if JAVA_HOME is set
if ($env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME "bin\java.exe"))) {
    $Java = Join-Path $env:JAVA_HOME "bin\java.exe"
} else {
    $Java = "java"
}

# Build arguments
$Arguments = $JavaOpts + @(
    "--module-path", $LibDir,
    "--class-path", $ConfDir,
    "--add-exports", "java.base/jdk.internal.misc=dongting.client",
    "-m", "dongting.ops/com.github.dtprj.dongting.boot.Bootstrap",
    "-c", (Join-Path $ConfDir "config.properties"),
    "-s", (Join-Path $ConfDir "servers.properties")
) + $args

# Start the application
& $Java $Arguments

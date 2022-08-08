/*
 * Copyright The Dongting Project
 *
 * The Dongting Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.github.dtprj.dongting.common.log;

public interface DtLog {
    boolean isDebugEnabled();
    void debug(String message);
    void debug(String format, Object arg);
    void debug(String format, Object arg1, Object arg2);
    void debug(String format, Object... args);
    void debug(String message, Throwable t);

    boolean isInfoEnabled();
    void info(String message);
    void info(String format, Object arg);
    void info(String format, Object arg1, Object arg2);
    void info(String format, Object... args);
    void info(String message, Throwable t);

    boolean isWarnEnabled();
    void warn(String message);
    void warn(String format, Object arg);
    void warn(String format, Object arg1, Object arg2);
    void warn(String format, Object... args);
    void warn(String message, Throwable t);

    boolean isErrorEnabled();
    void error(String message);
    void error(String format, Object arg);
    void error(String format, Object arg1, Object arg2);
    void error(String format, Object... args);
    void error(String message, Throwable t);
}

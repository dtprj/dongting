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

import org.slf4j.Logger;

class Slf4jLog implements DtLog {
    private final Logger log;

    public Slf4jLog(Logger log) {
        this.log = log;
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    @Override
    public void debug(String message) {
        log.debug(message);
    }

    @Override
    public void debug(String format, Object arg) {
        log.debug(format, arg);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        log.debug(format, arg1, arg2);
    }

    @Override
    public void debug(String format, Object... args) {
        log.debug(format, args);
    }

    @Override
    public void debug(String message, Throwable t) {
        log.debug(message, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return log.isInfoEnabled();
    }

    @Override
    public void info(String message) {
        log.info(message);
    }

    @Override
    public void info(String format, Object arg) {
        log.info(format, arg);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        log.info(format, arg1, arg2);
    }

    @Override
    public void info(String format, Object... args) {
        log.info(format, args);
    }

    @Override
    public void info(String message, Throwable t) {
        log.info(message, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return log.isWarnEnabled();
    }

    @Override
    public void warn(String message) {
        log.warn(message);
    }

    @Override
    public void warn(String format, Object arg) {
        log.warn(format, arg);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        log.warn(format, arg1, arg2);
    }

    @Override
    public void warn(String format, Object... args) {
        log.warn(format, args);
    }

    @Override
    public void warn(String message, Throwable t) {
        log.warn(message, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return log.isErrorEnabled();
    }

    @Override
    public void error(String message) {
        log.error(message);
    }

    @Override
    public void error(String format, Object arg) {
        log.error(format, arg);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        log.error(format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... args) {
        log.error(format, args);
    }

    @Override
    public void error(String message, Throwable t) {
        log.error(message, t);
    }

}

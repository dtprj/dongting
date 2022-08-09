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
package com.github.dtprj.dongting.log;

import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

class JdkLog implements DtLog {
    private final Logger log;

    public JdkLog(Logger log) {
        this.log = log;
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isLoggable(Level.FINE);
    }

    private void log(Level level, FormattingTuple ft) {
        LogRecord lr = new LogRecord(level, ft.getMessage());
        lr.setLoggerName(log.getName());
        lr.setThrown(ft.getThrowable());
        log.log(lr);
    }

    @Override
    public void debug(String message) {
        log.fine(message);
    }

    @Override
    public void debug(String format, Object arg) {
        if (log.isLoggable(Level.FINE)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.FINE, ft);
        }
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        if (log.isLoggable(Level.FINE)) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            log(Level.FINE, ft);
        }
    }

    @Override
    public void debug(String format, Object... args) {
        if (log.isLoggable(Level.FINE)) {
            FormattingTuple ft = MessageFormatter.format(format, args);
            log(Level.FINE, ft);
        }
    }

    @Override
    public void debug(String message, Throwable t) {
        log.log(Level.FINE, message, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return log.isLoggable(Level.INFO);
    }

    @Override
    public void info(String message) {
        log.info(message);
    }

    @Override
    public void info(String format, Object arg) {
        if (log.isLoggable(Level.INFO)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.INFO, ft);
        }
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        if (log.isLoggable(Level.INFO)) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            log(Level.INFO, ft);
        }
    }

    @Override
    public void info(String format, Object... args) {
        if (log.isLoggable(Level.INFO)) {
            FormattingTuple ft = MessageFormatter.format(format, args);
            log(Level.INFO, ft);
        }
    }

    @Override
    public void info(String message, Throwable t) {
        log.log(Level.INFO, message, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return log.isLoggable(Level.WARNING);
    }

    @Override
    public void warn(String message) {
        log.warning(message);
    }

    @Override
    public void warn(String format, Object arg) {
        if (log.isLoggable(Level.WARNING)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.WARNING, ft);
        }
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        if (log.isLoggable(Level.WARNING)) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            log(Level.WARNING, ft);
        }
    }

    @Override
    public void warn(String format, Object... args) {
        if (log.isLoggable(Level.WARNING)) {
            FormattingTuple ft = MessageFormatter.format(format, args);
            log(Level.WARNING, ft);
        }
    }

    @Override
    public void warn(String message, Throwable t) {
        log.log(Level.WARNING, message, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return log.isLoggable(Level.SEVERE);
    }

    @Override
    public void error(String message) {
        log.severe(message);
    }

    @Override
    public void error(String format, Object arg) {
        if (log.isLoggable(Level.SEVERE)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.SEVERE, ft);
        }
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        if (log.isLoggable(Level.SEVERE)) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            log(Level.SEVERE, ft);
        }
    }

    @Override
    public void error(String format, Object... args) {
        if (log.isLoggable(Level.SEVERE)) {
            FormattingTuple ft = MessageFormatter.format(format, args);
            log(Level.SEVERE, ft);
        }
    }

    @Override
    public void error(String message, Throwable t) {
        log.log(Level.SEVERE, message, t);
    }

}

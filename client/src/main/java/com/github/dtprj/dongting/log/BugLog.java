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

import com.github.dtprj.dongting.common.DtBugException;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public class BugLog {

    private static final DtLog log = DtLogs.getLogger(BugLog.class);

    private static volatile int count;
    private static volatile String firstError;

    private static synchronized void update(String msg, Throwable e) {
        count++;
        if (firstError == null) {
            ByteArrayOutputStream os = new ByteArrayOutputStream(256);
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
            if (msg != null && !msg.isEmpty()) {
                pw.println(msg);
            }
            e.printStackTrace(pw);
            pw.flush();
            firstError = os.toString();
        }
    }

    public static void log(Throwable e) {
        log.error("", e);
        update(null, e);
    }

    public static void log(String msg) {
        DtBugException e = new DtBugException(msg);
        log.error(msg, e);
        update(msg, e);
    }

    public static void logAndThrow(String msg) {
        DtBugException e = new DtBugException(msg);
        log.error(msg, e);
        update(msg, e);
        throw e;
    }

    public static void log(String format, Object... args) {
        log.error(format, args);

        FormattingTuple ft = MessageFormatter.arrayFormat(format, args);
        Throwable t = ft.getThrowable();
        if (t == null) {
            t = new DtBugException(ft.getMessage());
        }
        update(ft.getMessage(), t);
    }

    public static void log(String msg, Throwable t) {
        log.error(msg, t);
        update(msg, t);
    }

    public static int getCount() {
        return count;
    }

    public static String getFirstError() {
        return firstError;
    }

    public static void reset() {
        synchronized (BugLog.class) {
            count = 0;
            firstError = null;
        }
    }
}

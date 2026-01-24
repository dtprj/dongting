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

/**
 * @author huangli
 */
public class BugLog {

    private static final DtLog log = DtLogs.getLogger(BugLog.class);

    private static volatile int count;

    public static void log(Throwable e) {
        synchronized (BugLog.class) {
            count++;
        }
        log.error("", e);
    }

    public static void logAndThrow(Throwable e) {
        synchronized (BugLog.class) {
            count++;
        }
        log.error("", new DtBugException(e));
    }

    public static void log(String msg) {
        synchronized (BugLog.class) {
            count++;
        }
        log.error(msg, new DtBugException(msg));
    }

    public static void logAndThrow(String msg) {
        synchronized (BugLog.class) {
            count++;
        }
        DtBugException e = new DtBugException(msg);
        log.error(msg, e);
        throw e;
    }

    public static DtLog getLog() {
        synchronized (BugLog.class) {
            count++;
        }
        return log;
    }

    public static int getCount() {
        return count;
    }

    public static void reset() {
        synchronized (BugLog.class) {
            count = 0;
        }
    }
}

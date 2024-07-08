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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.log.BugLog;

/**
 * @author huangli
 */
public interface RaftCallback {

    void success(long raftIndex, Object result);

    void fail(Throwable ex);

    static void callSuccess(RaftCallback callback, long raftIndex, Object result) {
        try {
            callback.success(raftIndex, result);
        } catch (Throwable ex) {
            BugLog.getLog().error("RaftCallback error", ex);
        }
    }

    static void callFail(RaftCallback callback, Throwable ex) {
        try {
            callback.fail(ex);
        } catch (Throwable ex2) {
            BugLog.getLog().error("RaftCallback error", ex2);
        }
    }
}

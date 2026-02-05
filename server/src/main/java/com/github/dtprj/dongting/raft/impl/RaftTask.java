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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftInput;

/**
 * @author huangli
 */
public class RaftTask {

    public final int type;
    public final RaftInput input;
    public RaftCallback callback;
    public long localCreateNanos;
    public LogItem item;

    private boolean invokeCallback;

    boolean addPending;

    public RaftTask(int type, RaftInput input, RaftCallback callback) {
        this.type = type;
        this.input = input;
        this.callback = callback;
    }

    public void init(LogItem item, long localCreateNanos) {
        this.localCreateNanos = localCreateNanos;
        this.item = item;
    }

    public void callSuccess(Object r) {
        if (!invokeCallback) {
            try {
                RaftCallback.callSuccess(callback, item.index, r);
            } finally {
                callback = null;
                invokeCallback = true;
            }
        }
    }

    public void callFail(Throwable ex) {
        if (!invokeCallback) {
            try {
                RaftCallback.callFail(callback, ex);
            } finally {
                callback = null;
                invokeCallback = true;
            }
        }
    }
}

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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * @author huangli
 */
public class PendingStat {
    @SuppressWarnings("unused")
    private volatile int pendingRequests;
    @SuppressWarnings("unused")
    private volatile long pendingBytes;

    static final VarHandle PENDING_REQUESTS;
    static final VarHandle PENDING_BYTES;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            PENDING_REQUESTS = lookup.findVarHandle(PendingStat.class, "pendingRequests", int.class);
            PENDING_BYTES = lookup.findVarHandle(PendingStat.class, "pendingBytes", long.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    public void incrPlain(int requests, long bytes) {
        PENDING_REQUESTS.set(this, requests + (int) PENDING_REQUESTS.get(this));
        PENDING_BYTES.set(this, bytes + (long) PENDING_BYTES.get(this));
    }

    public void decrPlain(int requests, long bytes) {
        PENDING_REQUESTS.set(this, (int) PENDING_REQUESTS.get(this) - requests);
        PENDING_BYTES.set(this, (long) PENDING_BYTES.get(this) - bytes);
    }

    public int getPendingRequestsPlain() {
        return (int) PENDING_REQUESTS.get(this);
    }

    public long getPendingBytesPlain() {
        return (long) PENDING_BYTES.get(this);
    }

}

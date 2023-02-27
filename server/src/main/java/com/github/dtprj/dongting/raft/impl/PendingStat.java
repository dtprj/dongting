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

/**
 * @author huangli
 */
public class PendingStat {
    private int pendingRequests;
    private long pendingBytes;

    public void incrAndGetPendingRequests(int requests, long bytes) {
        pendingRequests += requests;
        pendingBytes += bytes;
    }

    public void decrAndGetPendingRequests(int requests, long bytes) {
        pendingRequests -= requests;
        pendingBytes -= bytes;
    }

    public int getPendingRequests() {
        return pendingRequests;
    }

    public long getPendingBytes() {
        return pendingBytes;
    }

}

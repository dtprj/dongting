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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.dtkv.WatchReq;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class WatchReqCallback extends PbCallback<WatchReqCallback> {
    public int groupId;
    public int operation;
    public long[] knownRaftIndexes;
    public int[] states;
    public byte[][] keys;

    private int knownRaftIndexesIndex = 0;
    private int keysIndex = 0;
    private int statesIndex = 0;

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case WatchReq.IDX_GROUP_ID:
                this.groupId = (int) value;
                break;
            case WatchReq.IDX_OPERATION:
                this.operation = (int) value;
                break;
            case WatchReq.IDX_STATES:
                this.states[statesIndex++] = (int) value;
                break;
            case WatchReq.IDX_KEYS_SIZE:
                int len = (int) value;
                this.knownRaftIndexes = new long[len];
                this.states = new int[len];
                this.keys = new byte[len][];
                break;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        if (index == WatchReq.IDX_KNOWN_RAFT_INDEXES) {
            knownRaftIndexes[knownRaftIndexesIndex++] = value;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index == WatchReq.IDX_KNOWN_RAFT_INDEXES) {
            byte[] b = parseBytes(buf, fieldLen, currentPos);
            if (b != null) {
                this.keys[keysIndex++] = b;
            }
        }
        return true;
    }

    @Override
    protected WatchReqCallback getResult() {
        return this;
    }
}

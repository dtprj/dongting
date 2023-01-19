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
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.pb.PbCallback;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author huangli
 */
//  uint32 term = 1;
//  uint32 leader_id = 2;
//  fixed64 prev_log_index = 3;
//  uint32 prev_log_term = 4;
//  repeated bytes entries = 5;
//  fixed64 leader_commit = 6;
public class AppendReqCallback extends PbCallback {

    private final ByteBufferPool heapPool;
    private int term;
    private int leaderId;
    private long prevLogIndex;
    private int prevLogTerm;
    // TODO batch
    private ByteBuffer log;
    private ArrayList<ByteBuffer> logs = new ArrayList<>();
    private long leaderCommit;

    public AppendReqCallback(ByteBufferPool heapPool) {
        this.heapPool = heapPool;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case 1:
                term = (int) value;
                break;
            case 2:
                leaderId = (int) value;
                break;
            case 4:
                prevLogTerm = (int) value;
                break;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        switch (index) {
            case 3:
                prevLogIndex = value;
                break;
            case 6:
                leaderCommit = value;
                break;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
        switch (index) {
            case 5:
                if (begin) {
                    log = ByteBuffer.allocate(len);
                }
                log.put(buf);
                if (end) {
                    log.flip();
                    logs.add(log);
                    log = null;
                }
                break;
        }
        return true;
    }

    @Override
    public AppendReqCallback getResult() {
        return this;
    }

    public int getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public ArrayList<ByteBuffer> getLogs() {
        return logs;
    }
}

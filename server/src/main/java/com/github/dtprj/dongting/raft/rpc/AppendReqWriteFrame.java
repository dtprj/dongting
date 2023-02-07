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

import com.github.dtprj.dongting.net.ZeroCopyWriteFrame;
import com.github.dtprj.dongting.pb.PbUtil;
import com.github.dtprj.dongting.raft.server.LogItem;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author huangli
 */
//message AppendEntriesReq {
//  uint32 term = 1;
//  uint32 leader_id = 2;
//  fixed64 prev_log_index = 3;
//  uint32 prev_log_term = 4;
//  repeated LogItem entries = 5;
//  fixed64 leader_commit = 6;
//}
//
//message LogItem {
//  uint32 type = 1;
//  uint32 term = 2;
//  fixed64 index = 3;
//  uint32 prev_log_term = 4;
//  bytes data = 5;
//}
public class AppendReqWriteFrame extends ZeroCopyWriteFrame {

    private int term;
    private int leaderId;
    private long prevLogIndex;
    private int prevLogTerm;
    private List<LogItem> logs;
    private long leaderCommit;

    @Override
    protected int calcAccurateBodySize() {
        int x = PbUtil.accurateUnsignedIntSize(1, term)
                + PbUtil.accurateUnsignedIntSize(2, leaderId)
                + PbUtil.accurateFix64Size(3, prevLogIndex)
                + PbUtil.accurateUnsignedIntSize(4, prevLogTerm)
                + PbUtil.accurateFix64Size(6, leaderCommit);
        if (logs != null) {
            for (LogItem item : logs) {
                int itemSize = computeItemSize(item);
                x += PbUtil.accurateLengthDelimitedSize(5, itemSize, false);
            }
        }
        return x;
    }

    private int computeItemSize(LogItem item) {
        int itemSize = 0;
        itemSize += PbUtil.accurateUnsignedIntSize(1, item.getType());
        itemSize += PbUtil.accurateUnsignedIntSize(2, item.getTerm());
        itemSize += PbUtil.accurateFix64Size(3, item.getIndex());
        itemSize += PbUtil.accurateUnsignedIntSize(4, item.getPrevLogTerm());
        if (item.getBuffer() != null) {
            itemSize += PbUtil.accurateLengthDelimitedSize(5, item.getBuffer().remaining(), true);
        }
        return itemSize;
    }

    @Override
    protected void encodeBody(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, term);
        PbUtil.writeUnsignedInt32(buf, 2, leaderId);
        PbUtil.writeFix64(buf, 3, prevLogIndex);
        PbUtil.writeUnsignedInt32(buf, 4, prevLogTerm);
        if (logs != null) {
            for (LogItem item : logs) {
                PbUtil.writeLengthDelimitedPrefix(buf, 5, computeItemSize(item), false);

                PbUtil.writeUnsignedInt32(buf, 1, item.getType());
                PbUtil.writeUnsignedInt32(buf, 2, item.getTerm());
                PbUtil.writeFix64(buf, 3, item.getIndex());
                PbUtil.writeUnsignedInt32(buf, 4, item.getPrevLogTerm());
                ByteBuffer logBuffer = item.getBuffer();
                if (logBuffer != null) {
                    PbUtil.writeLengthDelimitedPrefix(buf, 5, logBuffer.remaining(), true);
                    buf.put(logBuffer);
                }
            }
            logs = null;
        }
        PbUtil.writeFix64(buf, 6, leaderCommit);
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public void setPrevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public void setLeaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    public void setLogs(List<LogItem> logs) {
        this.logs = logs;
    }
}

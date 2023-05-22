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
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.server.LogItem;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author huangli
 */
//message AppendEntriesReq {
//  uint32 group_id = 1;
//  uint32 term = 2;
//  uint32 leader_id = 3;
//  fixed64 prev_log_index = 4;
//  uint32 prev_log_term = 5;
//  repeated LogItem entries = 6;
//  fixed64 leader_commit = 7;
//}
//
//message LogItem {
//  uint32 type = 1;
//  uint32 term = 2;
//  fixed64 index = 3;
//  uint32 prev_log_term = 4;
//  bytes data = 5;
//}
public class AppendReqWriteFrame extends WriteFrame {

    private final Encoder bodyEncoder;
    private int groupId;
    private int term;
    private int leaderId;
    private long prevLogIndex;
    private int prevLogTerm;
    private List<LogItem> logs;
    private long leaderCommit;

    public AppendReqWriteFrame(Encoder bodyEncoder) {
        this.bodyEncoder = bodyEncoder;
    }

    @Override
    protected int calcActualBodySize() {
        int x = PbUtil.accurateUnsignedIntSize(1, groupId)
                + PbUtil.accurateUnsignedIntSize(2, term)
                + PbUtil.accurateUnsignedIntSize(3, leaderId)
                + PbUtil.accurateFix64Size(4, prevLogIndex)
                + PbUtil.accurateUnsignedIntSize(5, prevLogTerm)
                + PbUtil.accurateFix64Size(7, leaderCommit);
        if (logs != null) {
            for (LogItem item : logs) {
                int itemSize = computeItemSize(item);
                x += PbUtil.accurateLengthDelimitedSize(6, itemSize);
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
        itemSize += PbUtil.accurateLengthDelimitedSize(5, item.getActualBodySize());
        return itemSize;
    }

    @Override
    protected void encodeBody(ByteBuffer buf, ByteBufferPool pool) {
        PbUtil.writeUnsignedInt32(buf, 1, groupId);
        PbUtil.writeUnsignedInt32(buf, 2, term);
        PbUtil.writeUnsignedInt32(buf, 3, leaderId);
        PbUtil.writeFix64(buf, 4, prevLogIndex);
        PbUtil.writeUnsignedInt32(buf, 5, prevLogTerm);
        if (logs != null) {
            for (LogItem item : logs) {
                PbUtil.writeLengthDelimitedPrefix(buf, 6, computeItemSize(item));

                PbUtil.writeUnsignedInt32(buf, 1, item.getType());
                PbUtil.writeUnsignedInt32(buf, 2, item.getTerm());
                PbUtil.writeFix64(buf, 3, item.getIndex());
                PbUtil.writeUnsignedInt32(buf, 4, item.getPrevLogTerm());
                int dataSize = item.getActualBodySize();
                if (dataSize > 0) {
                    PbUtil.writeLengthDelimitedPrefix(buf, 5, dataSize);
                    RefBuffer rbb = item.getBodyBuffer();
                    if (rbb != null) {
                        ByteBuffer src = rbb.getBuffer();
                        src.mark();
                        buf.put(src);
                        src.reset();
                        rbb.release();
                    } else {
                        bodyEncoder.encode(buf, item.getBody());
                    }
                }
            }
            logs = null;
        }
        PbUtil.writeFix64(buf, 7, leaderCommit);
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
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

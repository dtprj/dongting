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

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.store.LogHeader;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
//message AppendEntriesReq {
//int32 group_id = 1;
//int32 term = 2;
//int32 leader_id = 3;
//fixed64 prev_log_index = 4;
//int32 prev_log_term = 5;
//fixed64 leader_commit = 6;
//int32 logs_size = 7;
//bytes entries = 8;
//}
public class AppendReqWritePacket extends WritePacket {

    public int term;
    public int leaderId;
    public long prevLogIndex;
    public int prevLogTerm;
    public long leaderCommit;
    public List<RaftTask> logs;

    private int headerSize;
    private int bodySize;

    private static final int WRITE_HEADER = 0;
    private static final int WRITE_BODY = 1;
    private int writeStatus;
    private int encodeLogIndex;

    private final CRC32C crc = new CRC32C();

    public AppendReqWritePacket() {
    }

    @Override
    protected int calcActualBodySize() {
        headerSize = PbUtil.sizeOfInt32Field(AppendReq.IDX_GROUP_ID, groupId)
                + PbUtil.sizeOfInt32Field(AppendReq.IDX_TERM, term)
                + PbUtil.sizeOfInt32Field(AppendReq.IDX_LEADER_ID, leaderId)
                + PbUtil.sizeOfFix64Field(AppendReq.IDX_PREV_LOG_INDEX, prevLogIndex)
                + PbUtil.sizeOfInt32Field(AppendReq.IDX_PREV_LOG_TERM, prevLogTerm)
                + PbUtil.sizeOfFix64Field(AppendReq.IDX_LEADER_COMMIT, leaderCommit)
                + PbUtil.sizeOfInt32Field(AppendReq.IDX_LOGS_SIZE, logs == null ? 0 : logs.size());

        List<RaftTask> logs = this.logs;
        if (logs != null) {
            for (int size = logs.size(), i = 0; i < size; i++) {
                // assert itemSize > 0
                RaftTask item = logs.get(i);
                bodySize += LogHeader.computeTotalLen(item.reqData.bizHeaderSize, item.reqData.bizBodySize);
            }
        }

        // the header include len field of IDX_ENTRIES
        headerSize += PbUtil.sizeOfLenFieldPrefix(AppendReq.IDX_ENTRIES, bodySize);

        return headerSize + bodySize;
    }

    @Override
    protected boolean encodeBody(EncodeContext context, ByteBuffer dest) {
        switch (writeStatus) {
            case WRITE_HEADER:
                if (dest.remaining() < headerSize) {
                    return false;
                }
                PbUtil.writeInt32Field(dest, AppendReq.IDX_GROUP_ID, groupId);
                PbUtil.writeInt32Field(dest, AppendReq.IDX_TERM, term);
                PbUtil.writeInt32Field(dest, AppendReq.IDX_LEADER_ID, leaderId);
                PbUtil.writeFix64Field(dest, AppendReq.IDX_PREV_LOG_INDEX, prevLogIndex);
                PbUtil.writeInt32Field(dest, AppendReq.IDX_PREV_LOG_TERM, prevLogTerm);
                PbUtil.writeFix64Field(dest, AppendReq.IDX_LEADER_COMMIT, leaderCommit);
                PbUtil.writeInt32Field(dest, AppendReq.IDX_LOGS_SIZE, logs == null ? 0 : logs.size());
                PbUtil.writeLenFieldPrefix(dest, AppendReq.IDX_ENTRIES, bodySize);
                writeStatus = WRITE_BODY;
                // fall through
            case WRITE_BODY:
                while (true) {
                    RaftTask currentItem;
                    if (logs != null && encodeLogIndex < logs.size()) {
                        currentItem = logs.get(encodeLogIndex);
                    } else {
                        return true;
                    }
                    context.status = crc;
                    if (currentItem.encode(context, dest)) {
                        context.reset();
                        encodeLogIndex++;
                    } else {
                        return false;
                    }
                }
            default:
                throw new IllegalStateException("unknown write status " + writeStatus);
        }
    }

    @Override
    protected void doClean() {
        RaftUtil.releaseInputs(logs);
    }
}

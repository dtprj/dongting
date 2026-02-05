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
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.LogItem;

import java.nio.ByteBuffer;
import java.util.List;

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
//repeated LogItem entries = 8[packed=false];
//        }
//
//message LogItem {
//int32 type = 1;
//int32 bizType = 2;
//int32 term = 3;
//fixed64 index = 4;
//int32 prev_log_term = 5;
//fixed64 timestamp = 6;
//bytes header = 7;
//bytes body = 8;
//}
public class AppendReqWritePacket extends WritePacket {

    public int groupId;
    public int term;
    public int leaderId;
    public long prevLogIndex;
    public int prevLogTerm;
    public long leaderCommit;
    public List<LogItem> logs;

    private int headerSize;

    private static final int WRITE_HEADER = 0;
    private static final int WRITE_ITEM_HEADER = 1;
    private static final int WRITE_ITEM_BIZ_HEADER = 2;
    private static final int WRITE_ITEM_BIZ_BODY = 3;
    private int writeStatus;
    private int encodeLogIndex;

    private LogItem currentItem;

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
        int x = headerSize;
        if (logs != null) {
            for (LogItem item : logs) {
                int itemSize = computeItemSize(item);
                // assert itemSize > 0
                x += PbUtil.sizeOfLenFieldPrefix(AppendReq.IDX_ENTRIES, itemSize) + itemSize;
            }
        }
        return x;
    }

    private int computeItemSize(LogItem item) {
        int itemSize = item.pbItemSize;
        if (itemSize > 0) {
            return itemSize;
        }
        int itemHeaderSize = PbUtil.sizeOfInt32Field(LogItem.IDX_TYPE, item.type)
                + PbUtil.sizeOfInt32Field(LogItem.IDX_BIZ_TYPE, item.bizType)
                + PbUtil.sizeOfInt32Field(LogItem.IDX_TERM, item.term)
                + PbUtil.sizeOfFix64Field(LogItem.IDX_INDEX, item.index)
                + PbUtil.sizeOfInt32Field(LogItem.IDX_PREV_LOG_TERM, item.prevLogTerm)
                + PbUtil.sizeOfFix64Field(LogItem.IDX_TIMESTAMP, item.timestamp);
        itemSize = itemHeaderSize
                + EncodeUtil.sizeOf(LogItem.IDX_HEADER, item.getHeader())
                + EncodeUtil.sizeOf(LogItem.IDX_BODY, item.getBody());
        item.pbItemSize = itemSize;
        item.pbHeaderSize = itemHeaderSize;
        return itemSize;
    }

    @Override
    protected boolean encodeBody(EncodeContext context, ByteBuffer dest) {
        while (true) {
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
                    writeStatus = WRITE_ITEM_HEADER;
                    break;
                case WRITE_ITEM_HEADER:
                    if (logs != null && encodeLogIndex < logs.size()) {
                        currentItem = logs.get(encodeLogIndex);
                    } else {
                        return true;
                    }
                    if (dest.remaining() < PbUtil.sizeOfLenFieldPrefix(
                            AppendReq.IDX_ENTRIES, computeItemSize(currentItem)) + currentItem.pbHeaderSize) {
                        return false;
                    }
                    PbUtil.writeLenFieldPrefix(dest, AppendReq.IDX_ENTRIES, computeItemSize(currentItem));

                    PbUtil.writeInt32Field(dest, LogItem.IDX_TYPE, currentItem.type);
                    PbUtil.writeInt32Field(dest, LogItem.IDX_BIZ_TYPE, currentItem.bizType);
                    PbUtil.writeInt32Field(dest, LogItem.IDX_TERM, currentItem.term);
                    PbUtil.writeFix64Field(dest, LogItem.IDX_INDEX, currentItem.index);
                    PbUtil.writeInt32Field(dest, LogItem.IDX_PREV_LOG_TERM, currentItem.prevLogTerm);
                    PbUtil.writeFix64Field(dest, LogItem.IDX_TIMESTAMP, currentItem.timestamp);
                    writeStatus = WRITE_ITEM_BIZ_HEADER;
                    break;
                case WRITE_ITEM_BIZ_HEADER:
                    if (EncodeUtil.encode(context, dest, LogItem.IDX_HEADER, currentItem.getHeader())) {
                        writeStatus = WRITE_ITEM_BIZ_BODY;
                        break;
                    } else {
                        return false;
                    }
                case WRITE_ITEM_BIZ_BODY:
                    if (EncodeUtil.encode(context, dest, LogItem.IDX_BODY, currentItem.getBody())) {
                        writeStatus = WRITE_ITEM_HEADER;
                        currentItem = null;
                        encodeLogIndex++;
                        break;
                    } else {
                        return false;
                    }
                default:
                    throw new IllegalStateException("unknown write status " + writeStatus);
            }
        }
    }

    @Override
    protected void doClean() {
        RaftUtil.release(logs);
    }
}

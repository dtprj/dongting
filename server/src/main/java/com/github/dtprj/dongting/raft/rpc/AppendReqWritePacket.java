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
//  uint32 group_id = 1;
//  uint32 term = 2;
//  uint32 leader_id = 3;
//  fixed64 prev_log_index = 4;
//  uint32 prev_log_term = 5;
//  fixed64 leader_commit = 6;
//  repeated LogItem entries = 7;
//}
//
//message LogItem {
//  uint32 type = 1;
//  uint32 bizType = 2;
//  uint32 term = 3;
//  fixed64 index = 4;
//  uint32 prev_log_term = 5;
//  fixed64 timestamp = 6;
//  bytes header = 7;
//  bytes body = 8;
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
        headerSize = PbUtil.sizeOfInt32Field(1, groupId)
                + PbUtil.sizeOfInt32Field(2, term)
                + PbUtil.sizeOfInt32Field(3, leaderId)
                + PbUtil.sizeOfFix64Field(4, prevLogIndex)
                + PbUtil.sizeOfInt32Field(5, prevLogTerm)
                + PbUtil.sizeOfFix64Field(6, leaderCommit);
        int x = headerSize;
        if (logs != null) {
            for (LogItem item : logs) {
                int itemSize = computeItemSize(item);
                // assert itemSize > 0
                x += PbUtil.sizeOfLenFieldPrefix(7, itemSize) + itemSize;
            }
        }
        return x;
    }

    private int computeItemSize(LogItem item) {
        int itemSize = item.getPbItemSize();
        if (itemSize > 0) {
            return itemSize;
        }
        int itemHeaderSize = PbUtil.sizeOfInt32Field(1, item.getType())
                + PbUtil.sizeOfInt32Field(2, item.getBizType())
                + PbUtil.sizeOfInt32Field(3, item.getTerm())
                + PbUtil.sizeOfFix64Field(4, item.getIndex())
                + PbUtil.sizeOfInt32Field(5, item.getPrevLogTerm())
                + PbUtil.sizeOfFix64Field(6, item.getTimestamp());
        itemSize = itemHeaderSize
                + EncodeUtil.sizeOf(7, item.getHeader())
                + EncodeUtil.sizeOf(8, item.getBody());
        item.setPbItemSize(itemSize);
        item.setPbHeaderSize(itemHeaderSize);
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
                    PbUtil.writeInt32Field(dest, 1, groupId);
                    PbUtil.writeInt32Field(dest, 2, term);
                    PbUtil.writeInt32Field(dest, 3, leaderId);
                    PbUtil.writeFix64Field(dest, 4, prevLogIndex);
                    PbUtil.writeInt32Field(dest, 5, prevLogTerm);
                    PbUtil.writeFix64Field(dest, 6, leaderCommit);
                    writeStatus = WRITE_ITEM_HEADER;
                    break;
                case WRITE_ITEM_HEADER:
                    if (logs != null && encodeLogIndex < logs.size()) {
                        currentItem = logs.get(encodeLogIndex);
                    } else {
                        return true;
                    }
                    if (dest.remaining() < PbUtil.sizeOfLenFieldPrefix(
                            7, computeItemSize(currentItem)) + currentItem.getPbHeaderSize()) {
                        return false;
                    }
                    PbUtil.writeLenFieldPrefix(dest, 7, computeItemSize(currentItem));

                    PbUtil.writeInt32Field(dest, 1, currentItem.getType());
                    PbUtil.writeInt32Field(dest, 2, currentItem.getBizType());
                    PbUtil.writeInt32Field(dest, 3, currentItem.getTerm());
                    PbUtil.writeFix64Field(dest, 4, currentItem.getIndex());
                    PbUtil.writeInt32Field(dest, 5, currentItem.getPrevLogTerm());
                    PbUtil.writeFix64Field(dest, 6, currentItem.getTimestamp());
                    writeStatus = WRITE_ITEM_BIZ_HEADER;
                    break;
                case WRITE_ITEM_BIZ_HEADER:
                    if (EncodeUtil.encode(context, dest, 7, currentItem.getHeader())) {
                        writeStatus = WRITE_ITEM_BIZ_BODY;
                        break;
                    } else {
                        return false;
                    }
                case WRITE_ITEM_BIZ_BODY:
                    if (EncodeUtil.encode(context, dest, 8, currentItem.getBody())) {
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

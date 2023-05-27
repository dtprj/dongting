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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.ByteBufferWriteFrame;
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
//  fixed64 leader_commit = 6;
//  repeated LogItem entries = 7;
//}
//
//message LogItem {
//  uint32 type = 1;
//  uint32 term = 2;
//  fixed64 index = 3;
//  uint32 prev_log_term = 4;
//  fixed64 timestamp = 5;
//  bytes header = 6;
//  bytes body = 7;
//}
@SuppressWarnings("rawtypes")
public class AppendReqWriteFrame extends WriteFrame {

    private final EncodeContext context;
    private final Encoder headerEncoder;
    private final Encoder bodyEncoder;
    private int groupId;
    private int term;
    private int leaderId;
    private long prevLogIndex;
    private int prevLogTerm;
    private long leaderCommit;
    private List<LogItem> logs;

    private int headerSize;

    private static final int WRITE_HEADER = 0;
    private static final int WRITE_ITEM_HEADER = 1;
    private static final int WRITE_ITEM_BIZ_HEADER_LEN = 2;
    private static final int WRITE_ITEM_BIZ_HEADER = 3;
    private static final int WRITE_ITEM_BIZ_BODY_LEN = 4;
    private static final int WRITE_ITEM_BIZ_BODY = 5;
    private int writeStatus;
    private int encodeLogIndex;
    private int markedPosition;


    public AppendReqWriteFrame(EncodeContext context, Encoder headerEncoder, Encoder bodyEncoder) {
        this.context = context;
        this.headerEncoder = headerEncoder;
        this.bodyEncoder = bodyEncoder;
    }

    @Override
    protected int calcActualBodySize(EncodeContext context) {
        headerSize = PbUtil.accurateUnsignedIntSize(1, groupId)
                + PbUtil.accurateUnsignedIntSize(2, term)
                + PbUtil.accurateUnsignedIntSize(3, leaderId)
                + PbUtil.accurateFix64Size(4, prevLogIndex)
                + PbUtil.accurateUnsignedIntSize(5, prevLogTerm)
                + PbUtil.accurateFix64Size(6, leaderCommit);
        int x = headerSize;
        for (LogItem item : logs) {
            int itemSize = computeItemSize(item);
            x += PbUtil.accurateLengthDelimitedSize(7, itemSize);
        }
        return x;
    }

    private int computeItemSize(LogItem item) {
        int itemSize = item.getItemSize();
        if (itemSize > 0) {
            return itemSize;
        }
        int itemHeaderSize = PbUtil.accurateUnsignedIntSize(1, item.getType())
                + PbUtil.accurateUnsignedIntSize(2, item.getTerm())
                + PbUtil.accurateFix64Size(3, item.getIndex())
                + PbUtil.accurateUnsignedIntSize(4, item.getPrevLogTerm())
                + PbUtil.accurateFix64Size(5, item.getTimestamp());
        item.setItemHeaderSize(itemHeaderSize);
        itemSize = itemHeaderSize
                + PbUtil.accurateLengthDelimitedSize(6, item.getActualHeaderSize())
                + PbUtil.accurateLengthDelimitedSize(7, item.getActualBodySize());
        item.setItemSize(itemSize);
        return itemSize;
    }

    @Override
    protected boolean encodeBody(EncodeContext context, ByteBuffer buf) {
        LogItem item = null;
        while (true) {
            switch (writeStatus) {
                case WRITE_HEADER:
                    if (buf.remaining() < headerSize) {
                        return false;
                    }
                    PbUtil.writeUnsignedInt32(buf, 1, groupId);
                    PbUtil.writeUnsignedInt32(buf, 2, term);
                    PbUtil.writeUnsignedInt32(buf, 3, leaderId);
                    PbUtil.writeFix64(buf, 4, prevLogIndex);
                    PbUtil.writeUnsignedInt32(buf, 5, prevLogTerm);
                    PbUtil.writeFix64(buf, 6, leaderCommit);
                    writeStatus = WRITE_ITEM_HEADER;
                    break;
                case WRITE_ITEM_HEADER:
                    //noinspection ConstantValue
                    if (item == null) {
                        if (encodeLogIndex < logs.size()) {
                            item = logs.get(encodeLogIndex);
                        } else {
                            return true;
                        }
                    }
                    int require = PbUtil.accurateLengthDelimitedSize(7, computeItemSize(item))
                            + item.getItemHeaderSize();
                    if (buf.remaining() < require) {
                        return false;
                    }
                    PbUtil.writeLengthDelimitedPrefix(buf, 7, computeItemSize(item));

                    PbUtil.writeUnsignedInt32(buf, 1, item.getType());
                    PbUtil.writeUnsignedInt32(buf, 2, item.getTerm());
                    PbUtil.writeFix64(buf, 3, item.getIndex());
                    PbUtil.writeUnsignedInt32(buf, 4, item.getPrevLogTerm());
                    PbUtil.writeFix64(buf, 5, item.getTimestamp());
                    writeStatus = WRITE_ITEM_BIZ_HEADER_LEN;
                    break;
                case WRITE_ITEM_BIZ_HEADER_LEN:
                    assert item != null;
                    if (buf.remaining() < item.getActualHeaderSize()) {
                        return false;
                    }
                    PbUtil.writeLengthDelimitedPrefix(buf, 6, item.getActualHeaderSize());
                    markedPosition = -1;
                    writeStatus = WRITE_ITEM_BIZ_HEADER;
                    break;
                case WRITE_ITEM_BIZ_HEADER:
                    assert item != null;
                    if (!writeData(buf, item.getHeaderBuffer(), item.getHeader(), headerEncoder)) {
                        return false;
                    }
                    writeStatus = WRITE_ITEM_BIZ_BODY_LEN;
                    break;
                case WRITE_ITEM_BIZ_BODY_LEN:
                    assert item != null;
                    if (buf.remaining() < item.getActualBodySize()) {
                        return false;
                    }
                    PbUtil.writeLengthDelimitedPrefix(buf, 7, item.getActualBodySize());
                    markedPosition = -1;
                    writeStatus = WRITE_ITEM_BIZ_BODY;
                    break;
                case WRITE_ITEM_BIZ_BODY:
                    assert item != null;
                    if (!writeData(buf, item.getBodyBuffer(), item.getBody(), bodyEncoder)) {
                        return false;
                    }
                    item = null;
                    encodeLogIndex++;
                    writeStatus = WRITE_ITEM_HEADER;
                    break;
                default:
                    throw new IllegalStateException("unknown write status " + writeStatus);
            }
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean writeData(ByteBuffer dest, RefBuffer buffer, Object data, Encoder encoder) {
        if (!dest.hasRemaining()) {
            return false;
        }

        if (buffer != null) {
            ByteBuffer src = buffer.getBuffer();
            markedPosition = ByteBufferWriteFrame.copy(src, dest, markedPosition);
            if (markedPosition == src.limit()) {
                buffer.release();
                return true;
            } else {
                return false;
            }
        } else if (data != null) {
            //noinspection unchecked
            return encoder.encode(context, dest, data);
        } else {
            return true;
        }
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

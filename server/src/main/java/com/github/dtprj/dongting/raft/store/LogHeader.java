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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftReqData;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class LogHeader {
    public static final int TYPE_NORMAL = 0;
    public static final int TYPE_HEARTBEAT = 1;
    public static final int TYPE_PREPARE_CONFIG_CHANGE = 2;
    public static final int TYPE_DROP_CONFIG_CHANGE = 3;
    public static final int TYPE_COMMIT_CONFIG_CHANGE = 4;
    public static final int TYPE_LOG_READ = 5;

    // total len(4 bytes), include this 4 bytes
    // biz header len 4 bytes
    // body len 4 bytes
    // type 1 byte
    // bizType 1 byte
    // term 4 bytes
    // prevLogTerm 4 bytes
    // index 8 bytes
    // timestamp 8 bytes
    // header crc
    public static final int ITEM_HEADER_SIZE = 4 + 4 + 4 + 1 + 1 + 4 + 4 + 8 + 8 + 4;

    // negative value means end of file
    private static final int END_LEN_MAGIC = 0xF19A7BCB;

    public int totalLen;
    public int bizHeaderLen;
    public int bodyLen;
    public int type;
    public int bizType;
    public int term;
    public int prevLogTerm;
    public long index;
    public long timestamp;
    public int headerCrc;

    public LogHeader() {
    }

    public boolean isEndMagic() {
        return totalLen == END_LEN_MAGIC;
    }

    public void read(ByteBuffer buf) {
        totalLen = buf.getInt();
        bizHeaderLen = buf.getInt();
        bodyLen = buf.getInt();
        type = buf.get();
        bizType = buf.get();
        term = buf.getInt();
        prevLogTerm = buf.getInt();
        index = buf.getLong();
        timestamp = buf.getLong();
        headerCrc = buf.getInt();
    }

    public boolean readAndCheckCrc(CRC32C crc32c, ByteBuffer buf) {
        int start = buf.position();
        crc32c.reset();
        read(buf);
        RaftUtil.updateCrc(crc32c, buf, start, ITEM_HEADER_SIZE - 4);
        return headerCrc == ((int) crc32c.getValue());
    }

    public static int computeTotalLen(int bizHeaderLen, int bodyLen) {
        return ITEM_HEADER_SIZE + (bizHeaderLen == 0 ? 0 : bizHeaderLen + 4) + (bodyLen == 0 ? 0 : bodyLen + 4);
    }

    public static int writeHeader(CRC32C crc, ByteBuffer buffer, RaftTask rt) {
        boolean read = rt.type == TYPE_LOG_READ;
        int len;
        int bizHeaderSize;
        int bizBodySize;
        if (read) {
            len = ITEM_HEADER_SIZE;
            bizHeaderSize = 0;
            bizBodySize = 0;
        } else {
            RaftReqData reqData = rt.reqData;
            bizHeaderSize = reqData.bizHeaderSize;
            bizBodySize = reqData.bizBodySize;
            len = computeTotalLen(bizHeaderSize, bizBodySize);
        }
        int startPos = buffer.position();
        buffer.putInt(len);
        buffer.putInt(bizHeaderSize);
        buffer.putInt(bizBodySize);
        buffer.put((byte) rt.type);
        buffer.put((byte) rt.bizType);
        buffer.putInt(rt.term);
        buffer.putInt(rt.prevLogTerm);
        buffer.putLong(rt.index);
        buffer.putLong(rt.timestamp);
        crc.reset();
        RaftUtil.updateCrc(crc, buffer, startPos, ITEM_HEADER_SIZE - 4);
        buffer.putInt((int) crc.getValue());
        return len;
    }

    public static void writeEndHeader(CRC32C crc, ByteBuffer buffer) {
        int startPos = buffer.position();
        buffer.putInt(END_LEN_MAGIC);
        buffer.putInt(0);
        buffer.putInt(0);
        buffer.put((byte) 0);
        buffer.put((byte) 0);
        buffer.putInt(0);
        buffer.putInt(0);
        buffer.putLong(0L);
        buffer.putLong(0L);
        crc.reset();
        RaftUtil.updateCrc(crc, buffer, startPos, ITEM_HEADER_SIZE - 4);
        buffer.putInt((int) crc.getValue());
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean checkHeader(long filePos, long fileLen) {
        int expectTotalLen = LogHeader.computeTotalLen(bizHeaderLen, bodyLen);
        return type >= 0 && totalLen > 0 && bizHeaderLen >= 0 && bodyLen >= 0
                && expectTotalLen > 0
                && totalLen == expectTotalLen
                && filePos + expectTotalLen <= fileLen;
    }
}

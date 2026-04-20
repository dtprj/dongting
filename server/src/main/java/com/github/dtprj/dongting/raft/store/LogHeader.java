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

import com.github.dtprj.dongting.raft.impl.RaftUtil;

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

    public LogHeader(int type) {
        this.type = type;
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

    public void setLens(int bizHeaderLen, int bodyLen) {
        this.bizHeaderLen = bizHeaderLen;
        this.bodyLen = bodyLen;
        this.totalLen = computeTotalLen(bizHeaderLen, bodyLen);
    }

    public void computeCrc(CRC32C crc32c, ByteBuffer buf) {
        buf.clear();
        buf.putInt(totalLen);
        buf.putInt(bizHeaderLen);
        buf.putInt(bodyLen);
        buf.put((byte) type);
        buf.put((byte) bizType);
        buf.putInt(term);
        buf.putInt(prevLogTerm);
        buf.putLong(index);
        buf.putLong(timestamp);
        buf.flip();
        crc32c.reset();
        crc32c.update(buf);
        this.headerCrc = (int) crc32c.getValue();
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(totalLen);
        buffer.putInt(bizHeaderLen);
        buffer.putInt(bodyLen);
        buffer.put((byte) type);
        buffer.put((byte) bizType);
        buffer.putInt(term);
        buffer.putInt(prevLogTerm);
        buffer.putLong(index);
        buffer.putLong(timestamp);
        buffer.putInt(headerCrc);
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

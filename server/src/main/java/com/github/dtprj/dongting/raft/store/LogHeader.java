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
    // type(1) + bizType(1) + timestamp(6) packed into a single long (8 bytes)
    // term 4 bytes
    // prevLogTerm 4 bytes
    // index 8 bytes
    // header crc
    public static final int ITEM_HEADER_SIZE = 4 + 4 + 4 + 8 + 4 + 4 + 8 + 4;

    public static final int OFFSET_BIZ_HEADER_LEN = 4;
    public static final int OFFSET_BODY_LEN = 8;
    public static final int OFFSET_INDEX = 28;

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
        long packed = buf.getLong();
        type = (int) (packed >>> 56);
        bizType = (int) ((packed >>> 48) & 0xFF);
        timestamp = packed & 0xFFFF_FFFF_FFFFL;
        term = buf.getInt();
        prevLogTerm = buf.getInt();
        index = buf.getLong();
        headerCrc = buf.getInt();
    }

    public boolean readAndCheckCrc(CRC32C crc32c, ByteBuffer buf) {
        int start = buf.position();
        crc32c.reset();
        read(buf);
        RaftUtil.updateCrc(crc32c, buf, start, ITEM_HEADER_SIZE - 4);
        return headerCrc == ((int) crc32c.getValue());
    }

    static void readFields(ByteBuffer buf, RaftReqData data) {
        buf.getInt(); // skip totalLen, already set in data from buffer
        data.bizHeaderLen = buf.getInt();
        data.bodyLen = buf.getInt();
        long packed = buf.getLong();
        data.type = (int) (packed >>> 56);
        data.bizType = (int) ((packed >>> 48) & 0xFF);
        data.timestamp = packed & 0xFFFF_FFFF_FFFFL;
        data.term = buf.getInt();
        data.prevLogTerm = buf.getInt();
        data.index = buf.getLong();
    }

    public static int computeTotalLen(int bizHeaderLen, int bodyLen) {
        return ITEM_HEADER_SIZE + (bizHeaderLen == 0 ? 0 : bizHeaderLen + 4) + (bodyLen == 0 ? 0 : bodyLen + 4);
    }

    private void writeFields(ByteBuffer buf) {
        buf.putInt(totalLen);
        buf.putInt(bizHeaderLen);
        buf.putInt(bodyLen);
        buf.putLong(((type & 0xFFL) << 56) | ((bizType & 0xFFL) << 48) | (timestamp & 0xFFFF_FFFF_FFFFL));
        buf.putInt(term);
        buf.putInt(prevLogTerm);
        buf.putLong(index);
    }

    public void writeTo(ByteBuffer buffer) {
        writeFields(buffer);
        buffer.putInt(headerCrc);
    }

    public void writeTo(ByteBuffer buffer, int offset) {
        int oldPos = buffer.position();
        int oldLimit = buffer.limit();
        buffer.position(offset);
        buffer.limit(offset + ITEM_HEADER_SIZE);
        writeTo(buffer);
        buffer.limit(oldLimit);
        buffer.position(oldPos);
    }

    public static void writeAndComputeCrc(RaftReqData data, CRC32C crc32c, ByteBuffer buffer) {
        int start = buffer.position();
        writeFields(data, buffer);
        crc32c.reset();
        RaftUtil.updateCrc(crc32c, buffer, start, ITEM_HEADER_SIZE - 4);
        buffer.putInt((int) crc32c.getValue());
    }

    public static void writeAndComputeCrc(RaftReqData data, CRC32C crc32c, ByteBuffer buffer, int offset) {
        int oldPos = buffer.position();
        int oldLimit = buffer.limit();
        buffer.position(offset);
        buffer.limit(offset + ITEM_HEADER_SIZE);
        writeAndComputeCrc(data, crc32c, buffer);
        buffer.limit(oldLimit);
        buffer.position(oldPos);
    }

    private static void writeFields(RaftReqData data, ByteBuffer buf) {
        buf.putInt(data.totalLen);
        buf.putInt(data.bizHeaderLen);
        buf.putInt(data.bodyLen);
        buf.putLong(((data.type & 0xFFL) << 56) | ((data.bizType & 0xFFL) << 48)
                | (data.timestamp & 0xFFFF_FFFF_FFFFL));
        buf.putInt(data.term);
        buf.putInt(data.prevLogTerm);
        buf.putLong(data.index);
    }

    public static void writeEndHeader(CRC32C crc, ByteBuffer buffer) {
        int startPos = buffer.position();
        buffer.putInt(END_LEN_MAGIC);
        buffer.putInt(0);
        buffer.putInt(0);
        buffer.putLong(0L);
        buffer.putInt(0);
        buffer.putInt(0);
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

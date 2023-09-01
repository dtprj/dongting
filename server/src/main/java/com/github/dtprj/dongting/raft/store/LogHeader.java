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
import com.github.dtprj.dongting.raft.server.LogItem;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class LogHeader {
    // total len 4 bytes
    // context len 4 bytes
    // biz header len 4 bytes
    // body len 4 bytes
    // type 1 byte
    // bizType 1 byte
    // term 4 bytes
    // prevLogTerm 4 bytes
    // index 8 bytes
    // timestamp 8 bytes
    // header crc
    static final int ITEM_HEADER_SIZE = 4 + 4 + 4 + 4 + 1 + 1 + 4 + 4 + 8 + 8 + 4;

    // negative value means end of file
    private static final int END_LEN_MAGIC = 0xF19A7BCB;

    private final CRC32C crc32c = new CRC32C();

    int totalLen;
    int contextLen;
    int bizHeaderLen;
    int bodyLen;
    int type;
    int bizType;
    int term;
    int prevLogTerm;
    long index;
    long timestamp;

    int headerCrc;
    int expectCrc;

    public LogHeader() {
    }

    public boolean isEndMagic() {
        return totalLen == END_LEN_MAGIC;
    }

    public void read(ByteBuffer buf) {
        int start = buf.position();
        totalLen = buf.getInt();
        contextLen = buf.getInt();
        bizHeaderLen = buf.getInt();
        bodyLen = buf.getInt();
        type = buf.get();
        bizType = buf.get();
        term = buf.getInt();
        prevLogTerm = buf.getInt();
        index = buf.getLong();
        timestamp = buf.getLong();
        headerCrc = buf.getInt();

        CRC32C crc32c = this.crc32c;
        crc32c.reset();
        RaftUtil.updateCrc(crc32c, buf, start, ITEM_HEADER_SIZE - 4);
        expectCrc = (int) crc32c.getValue();
    }

    public boolean crcMatch() {
        return headerCrc == expectCrc;
    }

    public static int computeTotalLen(int contextLen, int bizHeaderLen, int bodyLen) {
        return ITEM_HEADER_SIZE
                + contextLen == 0 ? 0 : contextLen + 4
                + bizHeaderLen == 0 ? 0 : bizHeaderLen + 4
                + bodyLen == 0 ? 0 : bodyLen + 4;
    }

    public static void writeHeader(CRC32C crc, ByteBuffer buffer, LogItem log,
                                   int contextLen, int bizHeaderLen, int bodyLen) {
        int startPos = buffer.position();
        buffer.putInt(computeTotalLen(contextLen, bizHeaderLen, bodyLen));
        buffer.putInt(contextLen);
        buffer.putInt(bizHeaderLen);
        buffer.putInt(bodyLen);
        buffer.put((byte) log.getType());
        buffer.put((byte) log.getBizType());
        buffer.putInt(log.getTerm());
        buffer.putInt(log.getPrevLogTerm());
        buffer.putLong(log.getIndex());
        buffer.putLong(log.getTimestamp());
        crc.reset();
        RaftUtil.updateCrc(crc, buffer, startPos, ITEM_HEADER_SIZE - 4);
        buffer.putInt((int) crc.getValue());
    }

    public static void writeEndHeader(CRC32C crc, ByteBuffer buffer) {
        int startPos = buffer.position();
        buffer.putInt(END_LEN_MAGIC);
        buffer.putInt(0);
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
        int expectTotalLen = LogHeader.computeTotalLen(contextLen, bizHeaderLen, bodyLen);
        return type >= 0 && totalLen > 0 && bizHeaderLen >= 0 && bodyLen >= 0 && contextLen >= 0
                && expectTotalLen > 0
                && totalLen == expectTotalLen
                && filePos + expectTotalLen <= fileLen;
    }

    public void copy(LogItem li) {
        li.setIndex(index);
        li.setType(type);
        li.setBizType(bizType);
        li.setTerm(term);
        li.setPrevLogTerm(prevLogTerm);
        li.setTimestamp(timestamp);
    }
}

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

import com.github.dtprj.dongting.raft.server.ChecksumException;
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
    // term 4 bytes
    // prevLogTerm 4 bytes
    // index 8 bytes
    // timestamp 8 bytes
    // header crc
    static final int ITEM_HEADER_SIZE = 4 + 4 + 4 + 4 + 1 + 4 + 4 + 8 + 8 + 4;

    int totalLen;
    int contextLen;
    int bizHeaderLen;
    int bodyLen;
    int type;
    int term;
    int prevLogTerm;
    long index;
    long timestamp;

    public void read(CRC32C crc32c, ByteBuffer buf) {
        int start = buf.position();
        totalLen = buf.getInt();
        contextLen = buf.getInt();
        bizHeaderLen = buf.getInt();
        bodyLen = buf.getInt();
        type = buf.get();
        term = buf.getInt();
        prevLogTerm = buf.getInt();
        index = buf.getLong();
        timestamp = buf.getLong();
        int headerCrc = buf.getInt();
        crc32c.reset();
        LogFileQueue.updateCrc(crc32c, buf, start, ITEM_HEADER_SIZE - 4);
        if (headerCrc != crc32c.getValue()) {
            throw new ChecksumException("header crc32c not match");
        }
    }

    public static int computeTotalLen(int contextLen, int bizHeaderLen, int bodyLen) {
        return ITEM_HEADER_SIZE
                + contextLen == 0 ? 0 : contextLen + 4
                + bizHeaderLen == 0 ? 0 : bizHeaderLen + 4
                + bodyLen == 0 ? 0 : bodyLen + 4;
    }

    public static void writeHeader(CRC32C crc, ByteBuffer buffer, LogItem log,
                                   int bizHeaderLen, int contextLen, int bodyLen) {
        int startPos = buffer.position();
        buffer.putInt(computeTotalLen(bizHeaderLen, contextLen, bodyLen));
        buffer.putInt(contextLen);
        buffer.putInt(bizHeaderLen);
        buffer.putInt(bodyLen);
        buffer.put((byte) log.getType());
        buffer.putInt(log.getTerm());
        buffer.putInt(log.getPrevLogTerm());
        buffer.putLong(log.getIndex());
        buffer.putLong(log.getTimestamp());
        crc.reset();
        LogFileQueue.updateCrc(crc, buffer, startPos, ITEM_HEADER_SIZE);
        buffer.putInt((int) crc.getValue());
    }
}

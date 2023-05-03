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

import com.github.dtprj.dongting.raft.server.LogItem;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class LogHeader {
    // total len 4 bytes
    // biz header len 2 bytes
    // context len 4 bytes
    // type 1 byte
    // term 4 bytes
    // prevLogTerm 4 bytes
    // index 8 bytes
    // timestamp 8 bytes
    // header crc
    static final int ITEM_HEADER_SIZE = 4 + 2 + 4 + 1 + 4 + 4 + 8 + 8 + 4;

    int totalLen;
    int bizHeaderLen;
    int contextLen;
    int type;
    int term;
    int prevLogTerm;
    long index;
    long timestamp;
    int headerCrc;

    public void read(ByteBuffer buf) {
        totalLen = buf.getInt();
        bizHeaderLen = buf.getShort();
        contextLen = buf.getInt();
        type = buf.get();
        term = buf.getInt();
        prevLogTerm = buf.getInt();
        index = buf.getLong();
        timestamp = buf.getLong();
        headerCrc = buf.getInt();
    }

    public static int computeTotalLen(int bizHeaderLen, int contextLen, int bodyLen) {
        return ITEM_HEADER_SIZE
                + bizHeaderLen == 0 ? 0 : bizHeaderLen + 4
                + contextLen == 0 ? 0 : contextLen + 4
                + bodyLen == 0 ? 0 : bodyLen + 4;
    }

    public static void writeHeader(CRC32C crc, ByteBuffer buffer, LogItem log,
                                   int bizHeaderLen, int contextLen, int bodyLen) {
        int startPos = buffer.position();
        buffer.putInt(computeTotalLen(bizHeaderLen, contextLen, bodyLen));
        buffer.putShort((short) bizHeaderLen);
        buffer.putInt(contextLen);
        buffer.put((byte) log.getType());
        buffer.putInt(log.getTerm());
        buffer.putInt(log.getPrevLogTerm());
        buffer.putLong(log.getIndex());
        buffer.putLong(log.getTimestamp());
        crc.reset();
        LogFileQueue.updateCrc(crc, buffer, startPos, ITEM_HEADER_SIZE);
        buffer.putInt((int) crc.getValue());
    }

    public static int totalSize(int bodySize) {
        // header + body + crc32
        return ITEM_HEADER_SIZE + 4 + bodySize;
    }
}

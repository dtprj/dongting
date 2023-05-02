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

/**
 * @author huangli
 */
class LogHeader {
    // total len 4 bytes
    // head len 2 bytes
    // type 1 byte
    // term 4 bytes
    // prevLogTerm 4 bytes
    // index 8 bytes
    // timestamp 8 bytes
    static final int ITEM_HEADER_SIZE = 4 + 2 + 1 + 4 + 4 + 8 + 8;

    int totalLen;
    int headLen;
    int type;
    int term;
    int prevLogTerm;
    long index;
    long timestamp;

    public void read(ByteBuffer buf) {
        totalLen = buf.getInt();
        headLen = buf.getShort();
        type = buf.get();
        term = buf.getInt();
        prevLogTerm = buf.getInt();
        index = buf.getLong();
        timestamp = buf.getLong();
    }

    public static void writeHeader(ByteBuffer buffer, LogItem log, int totalLen) {
        buffer.putInt(totalLen);
        buffer.putShort((short) ITEM_HEADER_SIZE);
        buffer.put((byte) log.getType());
        buffer.putInt(log.getTerm());
        buffer.putInt(log.getPrevLogTerm());
        buffer.putLong(log.getIndex());
        buffer.putLong(log.getTimestamp());
    }

    public static int totalSize(int bodySize) {
        // header + body + crc32
        return ITEM_HEADER_SIZE + 4 + bodySize;
    }
}

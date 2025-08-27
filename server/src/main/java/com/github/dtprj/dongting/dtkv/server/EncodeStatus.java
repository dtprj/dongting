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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.common.DtUtil;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
class EncodeStatus {
    byte[] keyBytes;
    byte[] valueBytes;
    long createIndex;
    long createTime;
    long updateIndex;
    long updateTime;

    long uuid1;
    long uuid2;
    long ttlRaftIndex;
    long leaderTtlStartTime;
    long ttlMillis;

    private int offset;

    // createIndex(8) + createTime(8) + updateIndex(8) + updateTime(8) + uuid1(8) + uuid2(8)
    // + ttlRaftIndex(8) + leaderTtlStartTime(8) + ttlMillis(8) + keySize(4) + valueSize(4)
    private static final int HEADER_SIZE = 80;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);

    private int state;
    private static final int STATE_HEADER = 0;
    private static final int STATE_KEY = 1;
    private static final int STATE_VALUE = 2;

    public void reset() {
        keyBytes = null;
        valueBytes = null;
        offset = 0;
        createIndex = 0;
        createTime = 0;
        updateIndex = 0;
        updateTime = 0;

        uuid1 = 0;
        uuid2 = 0;
        ttlRaftIndex = 0;
        leaderTtlStartTime = 0;
        ttlMillis = 0;

        state = STATE_HEADER;
    }

    public boolean writeToBuffer(ByteBuffer dest) {
        int rest = dest.remaining();
        if (rest == 0) {
            return false;
        }
        switch (state) {
            case EncodeStatus.STATE_HEADER:
                if (rest < HEADER_SIZE - offset) {
                    if (offset == 0) {
                        // copy to temp dest
                        headerBuffer.clear();
                        writeHeader(headerBuffer);
                    }
                    dest.put(headerBuffer.array(), offset, rest);
                    offset += rest;
                    return false;
                } else {
                    if (offset > 0) {
                        dest.put(headerBuffer.array(), offset, HEADER_SIZE - offset);
                        offset = 0;
                    } else {
                        writeHeader(dest);
                    }
                    state = EncodeStatus.STATE_KEY;
                }
                // NOTICE: there is no break here
            case EncodeStatus.STATE_KEY:
                if (encode(dest, keyBytes)) {
                    state = EncodeStatus.STATE_VALUE;
                } else {
                    return false;
                }
                // NOTICE: there is no break here
            case EncodeStatus.STATE_VALUE:
                return encode(dest, valueBytes);
            default:
                throw new IllegalStateException();
        }
    }

    private void writeHeader(ByteBuffer buf) {
        buf.putLong(createIndex);
        buf.putLong(createTime);
        buf.putLong(updateIndex);
        buf.putLong(updateTime);
        buf.putLong(uuid1);
        buf.putLong(uuid2);
        buf.putLong(ttlRaftIndex);
        buf.putLong(leaderTtlStartTime);
        buf.putLong(ttlMillis);
        buf.putInt(keyBytes.length);
        if (valueBytes == null) {
            buf.putInt(0);
        } else {
            buf.putInt(valueBytes.length);
        }
    }

    private boolean encode(ByteBuffer dest, byte[] arr) {
        if (arr == null || arr.length == 0) {
            return true;
        }
        int rest = dest.remaining();
        if (rest == 0) {
            return false;
        }
        if (rest < arr.length - offset) {
            dest.put(arr, offset, rest);
            offset += rest;
            return false;
        } else {
            dest.put(arr, offset, arr.length - offset);
            offset = 0;
            return true;
        }
    }

    public boolean readFromBuffer(ByteBuffer buffer) {
        int rest = buffer.remaining();
        if (rest == 0) {
            return false;
        }
        switch (state) {
            case EncodeStatus.STATE_HEADER:
                if (rest < HEADER_SIZE - offset) {
                    // copy to temp buffer
                    buffer.get(headerBuffer.array(), offset, rest);
                    offset += rest;
                    return false;
                } else {
                    if (offset > 0) {
                        buffer.get(headerBuffer.array(), offset, HEADER_SIZE - offset);
                        headerBuffer.clear();
                        offset = 0;
                        readHeader(headerBuffer);
                    } else {
                        readHeader(buffer);
                    }
                    state = EncodeStatus.STATE_KEY;
                }
                // NOTICE: there is no break here
            case EncodeStatus.STATE_KEY:
                if (decode(buffer, keyBytes)) {
                    state = EncodeStatus.STATE_VALUE;
                } else {
                    return false;
                }
                //NOTICE: there is no break here
            case EncodeStatus.STATE_VALUE:
                return decode(buffer, valueBytes);
            default:
                throw new IllegalStateException();
        }
    }

    private void readHeader(ByteBuffer buf) {
        createIndex = buf.getLong();
        createTime = buf.getLong();
        updateIndex = buf.getLong();
        updateTime = buf.getLong();
        uuid1 = buf.getLong();
        uuid2 = buf.getLong();
        ttlRaftIndex = buf.getLong();
        leaderTtlStartTime = buf.getLong();
        ttlMillis = buf.getLong();

        int keySize = DtUtil.checkNotNegative(buf.getInt(), "keySize");

        keyBytes = new byte[keySize];

        int valueSize = DtUtil.checkNotNegative(buf.getInt(), "valueSize");
        if (valueSize > 0) {
            valueBytes = new byte[valueSize];
        }
    }

    private boolean decode(ByteBuffer src, byte[] arr) {
        if (arr == null || arr.length == 0) {
            return true;
        }
        int srcRest = src.remaining();
        if (srcRest == 0) {
            return false;
        }
        if (srcRest < arr.length - offset) {
            src.get(arr, offset, srcRest);
            offset += srcRest;
            return false;
        } else {
            src.get(arr, offset, arr.length - offset);
            offset = 0;
            return true;
        }
    }
}

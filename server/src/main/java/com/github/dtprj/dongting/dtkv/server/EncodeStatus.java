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

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
class EncodeStatus {
    byte[] keyBytes;
    byte[] valueBytes;
    long raftIndex;

    private int offset;

    private static final int HEADER_SIZE = 16;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);

    private int state;
    private static final int STATE_HEADER = 0;
    private static final int STATE_KEY = 1;
    private static final int STATE_VALUE = 2;

    public void reset() {
        keyBytes = null;
        valueBytes = null;
        offset = 0;
        raftIndex = 0;
        state = STATE_HEADER;
    }

    public boolean writeToBuffer(ByteBuffer buffer) {
        int rest = buffer.remaining();
        if (rest == 0) {
            return false;
        }
        switch (state) {
            case EncodeStatus.STATE_HEADER:
                if (rest < HEADER_SIZE - offset) {
                    if (offset == 0) {
                        // copy to temp buffer
                        headerBuffer.clear();
                        headerBuffer.putLong(raftIndex);
                        headerBuffer.putInt(keyBytes.length);
                        headerBuffer.putInt(valueBytes.length);
                    }
                    buffer.put(headerBuffer.array(), offset, rest);
                    offset += rest;
                    return false;
                } else {
                    if (offset > 0) {
                        buffer.put(headerBuffer.array(), offset, rest);
                        offset = 0;
                    } else {
                        buffer.putLong(raftIndex);
                        buffer.putInt(keyBytes.length);
                        buffer.putInt(valueBytes.length);
                    }
                    state = EncodeStatus.STATE_KEY;
                }
                // NOTICE: there is no break here
            case EncodeStatus.STATE_KEY:
                if (writeBytesToBuffer(buffer, keyBytes)) {
                    state = EncodeStatus.STATE_VALUE;
                } else {
                    return false;
                }
                // NOTICE: there is no break here
            case EncodeStatus.STATE_VALUE:
                return writeBytesToBuffer(buffer, valueBytes);
            default:
                throw new IllegalStateException();
        }
    }

    private boolean writeBytesToBuffer(ByteBuffer buffer, byte[] arr) {
        int rest = buffer.remaining();
        if (rest == 0) {
            return false;
        }
        if (rest < arr.length - offset) {
            buffer.put(arr, offset, rest);
            offset += rest;
            return false;
        } else {
            buffer.put(arr, offset, arr.length - offset);
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
                    int keyLen;
                    int valueLen;
                    if (offset > 0) {
                        buffer.get(headerBuffer.array(), offset, HEADER_SIZE - offset);
                        headerBuffer.clear();
                        offset = 0;
                        raftIndex = headerBuffer.getLong();
                        keyLen = headerBuffer.getInt();
                        valueLen = headerBuffer.getInt();
                    } else {
                        raftIndex = buffer.getLong();
                        keyLen = buffer.getInt();
                        valueLen = buffer.getInt();
                    }
                    keyBytes = new byte[keyLen];
                    valueBytes = new byte[valueLen];
                    state = EncodeStatus.STATE_KEY;
                }
                // NOTICE: there is no break here
            case EncodeStatus.STATE_KEY:
                if (readBytesFromBuffer(buffer, keyBytes)) {
                    state = EncodeStatus.STATE_VALUE;
                } else {
                    return false;
                }
                //NOTICE: there is no break here
            case EncodeStatus.STATE_VALUE:
                return readBytesFromBuffer(buffer, valueBytes);
            default:
                throw new IllegalStateException();
        }
    }

    private boolean readBytesFromBuffer(ByteBuffer buffer, byte[] arr) {
        int rest = buffer.remaining();
        if (rest == 0) {
            return false;
        }
        if (rest < arr.length - offset) {
            buffer.get(arr, offset, rest);
            offset += rest;
            return false;
        } else {
            buffer.get(arr, offset, arr.length - offset);
            offset = 0;
            return true;
        }
    }
}

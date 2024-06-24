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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.net.ByteBufferWriteFrame;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
class EncodeStatus {
    private final RefBufferFactory heapPool;
    byte[] keyBytes;
    int valueLen;
    RefBuffer valueBytes;
    long raftIndex;

    private int offset;

    private static final int HEADER_SIZE = 16;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);

    private int state;
    private static final int STATE_HEADER = 0;
    private static final int STATE_KEY = 1;
    private static final int STATE_VALUE = 2;

    public EncodeStatus(RefBufferFactory heapPool) {
        this.heapPool = heapPool;
    }

    public void reset() {
        keyBytes = null;
        valueBytes = null;
        valueLen = 0;
        offset = 0;
        raftIndex = 0;
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
                        headerBuffer.putLong(raftIndex);
                        headerBuffer.putInt(keyBytes.length);
                        headerBuffer.putInt(valueBytes.getBuffer().remaining());
                    }
                    dest.put(headerBuffer.array(), offset, rest);
                    offset += rest;
                    return false;
                } else {
                    if (offset > 0) {
                        dest.put(headerBuffer.array(), offset, HEADER_SIZE - offset);
                        offset = 0;
                    } else {
                        dest.putLong(raftIndex);
                        dest.putInt(keyBytes.length);
                        dest.putInt(valueBytes.getBuffer().remaining());
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
                return encode(dest, valueBytes.getBuffer());
            default:
                throw new IllegalStateException();
        }
    }

    private boolean encode(ByteBuffer dest, byte[] arr) {
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

    private boolean encode(ByteBuffer dest, ByteBuffer valueBuf) {
        if (dest.remaining() == 0) {
            return false;
        }
        offset = ByteBufferWriteFrame.copyFromHeapBuffer(valueBuf, dest, offset);
        if (offset < valueBuf.remaining()) {
            return false;
        } else {
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
                    valueBytes = heapPool.create(valueLen);
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
                return decode(buffer, valueBytes.getBuffer());
            default:
                throw new IllegalStateException();
        }
    }

    private boolean decode(ByteBuffer src, byte[] arr) {
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

    private boolean decode(ByteBuffer src, ByteBuffer valueBuf) {
        int srcRest = src.remaining();
        if (srcRest == 0) {
            return false;
        }
        if(srcRest < valueLen - offset) {
            src.put(valueBuf.array(), offset, srcRest);
            offset += srcRest;
            return false;
        } else {
            src.put(valueBuf.array(), offset, valueLen - offset);
            offset = 0;
            valueBuf.limit(valueLen);
            return true;
        }
    }
}

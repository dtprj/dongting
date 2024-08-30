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
package com.github.dtprj.dongting.codec;

import com.github.dtprj.dongting.common.DtUtil;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author huangli
 */
public class PbParser {

    private static final int STATUS_INIT = 1;
    private static final int STATUS_END = 2;
    private static final int STATUS_END_SKIP = 3;
    private static final int STATUS_PARSE_TAG = 4;
    private static final int STATUS_PARSE_FILED_LEN = 5;
    private static final int STATUS_PARSE_FILED_BODY = 6;
    private static final int STATUS_SKIP_REST = 7;

    PbCallback<?> callback;

    private int status;

    private int size;
    // not include first 4 bytes of protobuf len
    private int parsedBytes;

    private int pendingBytes;
    private int fieldType;
    private int fieldIndex;
    private int fieldLen;
    private long tempValue;

    private DecodeContext context;

    public PbParser() {
        this.status = STATUS_INIT;
    }

    void reset() {
        switch (status) {
            case STATUS_INIT:
            case STATUS_END:
            case STATUS_END_SKIP:
                break;
            default:
                callEnd(callback, false);
        }

        /*
         these fields are not necessary to reset

         this.fieldType = 0;
         this.fieldIndex = 0;
         this.fieldLen = 0;
         this.tempValue = 0;
        */
    }

    public void prepareNext(DecodeContext context, PbCallback<?> callback, int size) {
        this.callback = Objects.requireNonNull(callback);
        this.context = Objects.requireNonNull(context);
        this.size = DtUtil.checkNotNegative(size, "size");
        this.status = STATUS_INIT;

        context.status = null;

        callback.context = context;
        callback.parser = this;

        this.pendingBytes = 0;
        this.parsedBytes = 0;

        this.fieldType = 0;
        this.fieldIndex = 0;
        this.fieldLen = 0;
        this.tempValue = 0;
    }

    public boolean isFinished() {
        return status == STATUS_END || status == STATUS_END_SKIP;
    }

    public boolean shouldSkip() {
        return status == STATUS_END_SKIP || status == STATUS_SKIP_REST;
    }

    public Object parse(ByteBuffer buf) {
        if (status == STATUS_END || status == STATUS_END_SKIP) {
            throw new PbException("parser is finished");
        }
        try {
            return parse0(buf);
        } catch (RuntimeException | Error e) {
            callEnd(callback, false);
            throw e;
        }
    }

    private Object parse0(ByteBuffer buf) {
        int remain = buf.remaining();
        PbCallback<?> callback = this.callback;
        while (true) {
            switch (this.status) {
                case STATUS_INIT:
                    callBegin(callback, size);
                    if (size == 0) {
                        return callEnd(callback, true);
                    }
                    break;
                case STATUS_PARSE_TAG:
                case STATUS_PARSE_FILED_LEN:
                    remain = parseVarInt(buf, remain);
                    if (status != STATUS_PARSE_FILED_BODY || fieldLen > 0) {
                        break;
                    }
                    // go down to ensure empty body will invoke callEnd()
                case STATUS_PARSE_FILED_BODY:
                    remain = onStatusParseFieldBody(buf, callback, remain);
                    if (size == parsedBytes && (status == STATUS_PARSE_TAG || status == STATUS_SKIP_REST)) {
                        return callEnd(callback, status == STATUS_PARSE_TAG);
                    }
                    break;
                case STATUS_SKIP_REST:
                    int skipCount = Math.min(this.size - this.parsedBytes, buf.remaining());
                    buf.position(buf.position() + skipCount);
                    if (this.parsedBytes + skipCount == this.size) {
                        return callEnd(callback, false);
                    } else {
                        this.parsedBytes += skipCount;
                    }
                    remain -= skipCount;
                    break;
                default:
                    throw new PbException("invalid status: " + status);
            }
            if (remain <= 0) {
                return null;
            }
        }
    }

    private Object callEnd(PbCallback<?> callback, boolean success) {
        if (this.status > STATUS_END_SKIP) {
            try {
                if (success) {
                    success = callback.end(true);
                    return callback.getResult();
                } else {
                    callback.end(false);
                    return null;
                }
            } finally {
                this.status = success ? STATUS_END : STATUS_END_SKIP;
                this.pendingBytes = 0;
                this.size = 0;
                this.parsedBytes = 0;

                context.status = null;
                callback.context = null;
                callback.parser = null;

                this.callback = null;
            }
        }
        return null;
    }

    private void callBegin(PbCallback<?> callback, int len) {
        callback.begin(len);
        this.status = STATUS_PARSE_TAG;
    }

    private int parseVarInt(ByteBuffer buf, int remain) {
        int value = 0;
        int bitIndex = 0;
        int pendingBytes = this.pendingBytes;
        if (pendingBytes > 0) {
            value = (int) this.tempValue;
            bitIndex = pendingBytes * 7;
        }

        // max 5 bytes for 32bit number in proto buffer
        final int MAX_BYTES = 5;
        int i = 1;
        int size = this.size;
        int parsedBytes = this.parsedBytes;
        for (; i <= remain; i++) {
            int x = buf.get();
            value |= (x & 0x7F) << bitIndex;
            if (x >= 0) {
                // first bit is 0, read complete
                if (pendingBytes + i > MAX_BYTES) {
                    throw new PbException("var int too long: " + (pendingBytes + i + 1));
                }
                parsedBytes += i;
                if (parsedBytes > size) {
                    throw new PbException("size exceed " + size);
                }

                /////////////////////////////////////
                switch (status) {
                    case STATUS_PARSE_TAG:
                        afterTagParsed(value);
                        break;
                    case STATUS_PARSE_FILED_LEN:
                        if (value < 0) {
                            throw new PbException("bad field len: " + fieldLen);
                        }
                        if (parsedBytes + value > size) {
                            throw new PbException("field length overflow. len=" + value + ",index=" + fieldIndex);
                        }
                        this.fieldLen = value;
                        this.status = STATUS_PARSE_FILED_BODY;
                        break;
                    default:
                        throw new PbException("invalid status: " + status);
                }
                this.parsedBytes = parsedBytes;
                this.pendingBytes = 0;
                /////////////////////////////////////
                return remain - i;
            } else {
                bitIndex += 7;
            }
        }
        pendingBytes += remain;
        parsedBytes += remain;
        if (pendingBytes >= MAX_BYTES) {
            throw new PbException("var int too long, at least " + pendingBytes);
        }
        if (parsedBytes >= size) {
            throw new PbException("size exceed, at least " + size);
        }
        this.tempValue = value;
        this.pendingBytes = pendingBytes;
        this.parsedBytes = parsedBytes;
        return 0;
    }

    private void afterTagParsed(int value) {
        int type = value & 0x07;
        this.fieldType = type;
        value = value >>> 3;
        if (value == 0) {
            throw new PbException("bad index:" + fieldIndex);
        }
        this.fieldIndex = value;

        switch (type) {
            case PbUtil.TYPE_VAR_INT:
            case PbUtil.TYPE_FIX64:
            case PbUtil.TYPE_FIX32:
                this.status = STATUS_PARSE_FILED_BODY;
                break;
            case PbUtil.TYPE_LENGTH_DELIMITED:
                this.status = STATUS_PARSE_FILED_LEN;
                break;
            default:
                throw new PbException("type not support:" + type);
        }
    }

    private int parseVarLong(ByteBuffer buf, PbCallback<?> callback, int remain) {
        long value = 0;
        int bitIndex = 0;
        int pendingBytes = this.pendingBytes;
        if (pendingBytes > 0) {
            value = this.tempValue;
            bitIndex = pendingBytes * 7;
        }

        // max 10 bytes for 64bit number in proto buffer
        final int MAX_BYTES = 10;
        int i = 1;
        int size = this.size;
        int parsedBytes = this.parsedBytes;
        for (; i <= remain; i++) {
            int x = buf.get();
            value |= (x & 0x7FL) << bitIndex;
            if (x >= 0) {
                // first bit is 0, read complete
                if (pendingBytes + i > MAX_BYTES) {
                    throw new PbException("var long too long: " + (pendingBytes + i + 1));
                }
                parsedBytes += i;
                if (parsedBytes > size) {
                    throw new PbException("size exceed " + size);
                }

                if (callback.readVarNumber(this.fieldIndex, value)) {
                    this.status = STATUS_PARSE_TAG;
                } else {
                    this.status = STATUS_SKIP_REST;
                }

                this.pendingBytes = 0;
                this.parsedBytes = parsedBytes;
                return remain - i;
            } else {
                bitIndex += 7;
            }
        }
        pendingBytes += remain;
        parsedBytes += remain;
        if (pendingBytes >= MAX_BYTES) {
            throw new PbException("var long too long, at least " + pendingBytes);
        }
        if (parsedBytes >= size) {
            throw new PbException("size exceed, at least " + size);
        }
        this.tempValue = value;
        this.pendingBytes = pendingBytes;
        this.parsedBytes = parsedBytes;
        return 0;
    }

    private int onStatusParseFieldBody(ByteBuffer buf, PbCallback<?> callback, int remain) {
        switch (this.fieldType) {
            case PbUtil.TYPE_VAR_INT:
                remain = parseVarLong(buf, callback, remain);
                break;
            case PbUtil.TYPE_FIX64:
                remain = parseBodyFixedNumber(buf, callback, remain, 8);
                break;
            case PbUtil.TYPE_FIX32:
                remain = parseBodyFixedNumber(buf, callback, remain, 4);
                break;
            case PbUtil.TYPE_LENGTH_DELIMITED:
                remain = parseBodyLenDelimited(buf, callback, remain);
                break;
            default:
                throw new PbException("type not support:" + this.fieldType);
        }
        return remain;
    }

    private int parseBodyLenDelimited(ByteBuffer buf, PbCallback<?> callback, int remain) {
        int fieldLen = this.fieldLen;
        if (remain == 0 && fieldLen > 0) {
            return 0;
        }
        int needRead = fieldLen - pendingBytes;
        int actualRead = Math.min(needRead, remain);
        int start = buf.position();
        int end = start + actualRead;
        int oldLimit = buf.limit();
        buf.limit(end);
        boolean result = callback.readBytes(this.fieldIndex, buf, fieldLen, pendingBytes);
        buf.limit(oldLimit);
        buf.position(end);
        parsedBytes += actualRead;
        if (result) {
            if (needRead == actualRead) {
                pendingBytes = 0;
                status = STATUS_PARSE_TAG;
            } else {
                pendingBytes += actualRead;
            }
        } else {
            pendingBytes = 0;
            status = STATUS_SKIP_REST;
        }
        return remain - actualRead;
    }

    private int parseBodyFixedNumber(ByteBuffer buf, PbCallback<?> callback, int remain, int len) {
        int pendingBytes = this.pendingBytes;
        if (pendingBytes == 0 && remain >= len) {
            long value;
            if (len == 4) {
                value = Integer.reverseBytes(buf.getInt());
            } else {
                value = Long.reverseBytes(buf.getLong());
            }
            callbackOnReadFixNumber(callback, len, value);
            this.pendingBytes = 0;
            this.parsedBytes += len;
            return remain - len;
        }
        long value = 0;
        if (pendingBytes > 0) {
            value = this.tempValue;
        }
        int restLen = len - pendingBytes;
        int bitIndex = pendingBytes << 3; // multiply 8
        if (remain >= restLen) {
            for (int i = 0; i < restLen; i++) {
                value |= (buf.get() & 0xFFL) << bitIndex;
                bitIndex += 8;
            }
            callbackOnReadFixNumber(callback, len, value);
            this.pendingBytes = 0;
            this.parsedBytes += restLen;
            return remain - restLen;
        } else {
            for (int i = 0; i < remain; i++) {
                value |= (buf.get() & 0xFFL) << bitIndex;
                bitIndex += 8;
            }
            this.pendingBytes = pendingBytes + remain;
            this.parsedBytes = this.parsedBytes + remain;
            this.tempValue = value;
            return 0;
        }
    }

    private void callbackOnReadFixNumber(PbCallback<?> callback, int len, long value) {
        boolean r;
        if (len == 4) {
            r = callback.readFix32(this.fieldIndex, (int) value);
        } else {
            r = callback.readFix64(this.fieldIndex, value);
        }
        if (r) {
            this.status = STATUS_PARSE_TAG;
        } else {
            this.status = STATUS_SKIP_REST;
        }
    }
}

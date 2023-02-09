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
package com.github.dtprj.dongting.pb;

import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.ObjUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author huangli
 */
public class PbParser {

    private static final DtLog log = DtLogs.getLogger(PbParser.class);

    private static final int STATUS_PARSE_PB_LEN = 1;
    private static final int STATUS_PARSE_TAG = 2;
    private static final int STATUS_PARSE_FILED_LEN = 3;
    private static final int STATUS_PARSE_FILED_BODY = 4;
    private static final int STATUS_SKIP_REST = 5;

    private PbCallback callback;

    private int maxFrame;

    private int status;

    private int frameLen;
    // not include first 4 bytes of protobuf len
    private int parsedBytes;

    private int pendingBytes;
    private int fieldType;
    private int fieldIndex;
    private int fieldLen;
    private long tempValue;

    private PbParser nestedParser;

    private PbParser(PbCallback callback, boolean multi, int maxFrameOrPbLen) {
        Objects.requireNonNull(callback);
        if (multi) {
            ObjUtil.checkPositive(maxFrameOrPbLen, "maxFrame");
            this.callback = callback;
            this.maxFrame = maxFrameOrPbLen;
            this.status = STATUS_PARSE_PB_LEN;
        } else {
            ObjUtil.checkPositive(maxFrameOrPbLen, "pbLen");
            this.callback = callback;
            this.maxFrame = 0;
            this.frameLen = maxFrameOrPbLen;
            callBegin(callback, maxFrameOrPbLen);
        }
    }

    private boolean isSingle() {
        return maxFrame == 0;
    }

    public static PbParser multiParser(PbCallback callback, int maxFrame) {
        return new PbParser(callback, true, maxFrame);
    }

    public static PbParser singleParser(PbCallback callback, int pbLen) {
        return new PbParser(callback, false, pbLen);
    }

    public void resetMulti(PbCallback callback, int maxFrame) {
        reset(callback, true, maxFrame);
    }

    public void resetSingle(PbCallback callback, int pbLen) {
        reset(callback, false, pbLen);
    }

    private void reset(PbCallback callback, boolean multi, int maxFrameOrPbLen) {
        this.callback = callback;
        this.parsedBytes = 0;
        this.pendingBytes = 0;
        this.fieldType = 0;
        this.fieldIndex = 0;
        this.fieldLen = 0;
        this.tempValue = 0;

        if (multi) {
            this.maxFrame = maxFrameOrPbLen;
            this.frameLen = 0;
            this.status = STATUS_PARSE_PB_LEN;
        } else {
            this.maxFrame = 0;
            this.frameLen = maxFrameOrPbLen;
            callBegin(callback, maxFrameOrPbLen);
        }
    }

    public void parse(ByteBuffer buf) {
        int remain = buf.remaining();
        PbCallback callback = this.callback;
        while (true) {
            switch (this.status) {
                case STATUS_PARSE_PB_LEN:
                    remain = onStatusParsePbLen(buf, callback, remain);
                    break;
                case STATUS_PARSE_TAG:
                case STATUS_PARSE_FILED_LEN:
                    remain = parseVarInt(buf, remain);
                    if (status != STATUS_PARSE_FILED_BODY) {
                        break;
                    }
                case STATUS_PARSE_FILED_BODY:
                    remain = onStatusParseFieldBody(buf, callback, remain);
                    break;
                case STATUS_SKIP_REST:
                    int skipCount = Math.min(this.frameLen - this.parsedBytes, buf.remaining());
                    buf.position(buf.position() + skipCount);
                    if (this.parsedBytes + skipCount == this.frameLen) {
                        callEnd(callback, false);
                    } else {
                        this.parsedBytes += skipCount;
                    }
                    remain -= skipCount;
                    break;
            }
            if (remain == 0) {
                return;
            }
        }
    }

    private void callEnd(PbCallback callback, boolean success) {
        try {
            callback.end(success);
        } catch (Throwable e) {
            log.error("proto buffer parse callback end() fail: {}", e.toString());
        }
        if (isSingle()) {
            this.callback = null;
        }
        this.status = STATUS_PARSE_PB_LEN;
        this.pendingBytes = 0;
        this.frameLen = 0;
        this.parsedBytes = 0;
    }

    private void callBegin(PbCallback callback, int len) {
        try {
            callback.begin(len, this);
            this.status = STATUS_PARSE_TAG;
        } catch (Throwable e) {
            log.error("proto buffer parse callback begin() fail: {}", e.toString());
            this.status = STATUS_SKIP_REST;
        }
        if (len == 0) {
            callEnd(callback, this.status == STATUS_PARSE_TAG);
        }
    }

    private int onStatusParsePbLen(ByteBuffer buf, PbCallback callback, int remain) {
        if (isSingle()) {
            throw new DtException("single parser can't reuse");
        }

        // read buffer is little endian.
        // however the length field is out of proto buffer data, and it's big endian
        if (pendingBytes == 0 && remain >= 4) {
            int len = buf.getInt();
            len = Integer.reverseBytes(len);
            if (len < 0 || len > maxFrame) {
                throw new PbException("maxFrameSize exceed: max=" + maxFrame + ", actual=" + len);
            }
            frameLen = len;
            callBegin(callback, len);
            return remain - 4;
        }
        int restLen = 4 - pendingBytes;
        if (remain >= restLen) {
            for (int i = 0; i < restLen; i++) {
                frameLen = (frameLen << 8) | (0xFF & buf.get());
            }
            if (frameLen < 0 || frameLen > maxFrame) {
                throw new PbException("maxFrameSize exceed: max=" + maxFrame + ", actual=" + frameLen);
            }
            pendingBytes = 0;
            callBegin(callback, frameLen);
            return remain - restLen;
        } else {
            for (int i = 0; i < remain; i++) {
                frameLen = (frameLen << 8) | (0xFF & buf.get());
            }
            pendingBytes += remain;
            return 0;
        }
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
        int frameLen = this.frameLen;
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
                if (parsedBytes > frameLen) {
                    throw new PbException("frame exceed " + frameLen);
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
                        if (parsedBytes + value > frameLen) {
                            throw new PbException("field length overflow frame length:" + value);
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
        if (parsedBytes >= frameLen) {
            throw new PbException("frame exceed, at least " + frameLen);
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

    private int parseVarLong(ByteBuffer buf, PbCallback callback, int remain) {
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
        int frameLen = this.frameLen;
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
                if (parsedBytes > frameLen) {
                    throw new PbException("frame exceed " + frameLen);
                }

                try {
                    if (callback.readVarNumber(this.fieldIndex, value)) {
                        this.status = STATUS_PARSE_TAG;
                    } else {
                        this.status = STATUS_SKIP_REST;
                    }
                } catch (Throwable e) {
                    log.error("proto buffer parse callback readVarInt() fail. fieldIndex={}, error={}", this.fieldIndex, e.toString());
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
        if (parsedBytes >= frameLen) {
            throw new PbException("frame exceed, at least " + frameLen);
        }
        this.tempValue = value;
        this.pendingBytes = pendingBytes;
        this.parsedBytes = parsedBytes;
        return 0;
    }

    private int onStatusParseFieldBody(ByteBuffer buf, PbCallback callback, int remain) {
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
        int status = this.status;
        if (frameLen == parsedBytes && (status == STATUS_PARSE_TAG || status == STATUS_SKIP_REST)) {
            callEnd(callback, status == STATUS_PARSE_TAG);
        }
        return remain;
    }

    private int parseBodyLenDelimited(ByteBuffer buf, PbCallback callback, int remain) {
        int fieldLen = this.fieldLen;
        int pendingBytes = this.pendingBytes;
        int needRead = fieldLen - pendingBytes;
        int actualRead = Math.min(needRead, remain);
        int start = buf.position();
        int end = start + actualRead;
        int limit = buf.limit();
        buf.limit(end);
        boolean result = false;
        try {
            result = callback.readBytes(this.fieldIndex, buf, fieldLen,
                    pendingBytes == 0, needRead == actualRead);
        } catch (Throwable e) {
            log.error("proto buffer parse callback readBytes() fail. fieldIndex={}, error={}", this.fieldIndex, e.toString());
        } finally {
            buf.limit(limit);
            buf.position(end);
            parsedBytes += actualRead;
            remain -= actualRead;
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
            this.pendingBytes = pendingBytes;
        }
        return remain;
    }

    private int parseBodyFixedNumber(ByteBuffer buf, PbCallback callback, int remain, int len) {
        int pendingBytes = this.pendingBytes;
        if (pendingBytes == 0 && remain >= len) {
            long value;
            if (len == 4) {
                value = buf.getInt();
            } else {
                value = buf.getLong();
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

    private void callbackOnReadFixNumber(PbCallback callback, int len, long value) {
        boolean r;
        try {
            if (len == 4) {
                r = callback.readFix32(this.fieldIndex, (int) value);
            } else {
                r = callback.readFix64(this.fieldIndex, value);
            }
        } catch (Throwable e) {
            log.error("proto buffer parse callback readFixInt()/readFixLong() fail. fieldIndex={}, error={}", this.fieldIndex, e.toString());
            r = false;
        }
        if (r) {
            this.status = STATUS_PARSE_TAG;
        } else {
            this.status = STATUS_SKIP_REST;
        }
    }

    public PbCallback getCallback() {
        return callback;
    }

    public PbParser getNestedParser() {
        return nestedParser;
    }

    public PbParser createOrGetNestedParserSingle(PbCallback callback, int pbLen) {
        PbParser nestedParser = this.nestedParser;
        if (nestedParser == null) {
            nestedParser = singleParser(callback, pbLen);
            this.nestedParser = nestedParser;
        } else {
            nestedParser.resetSingle(callback, pbLen);
        }
        return nestedParser;
    }
}

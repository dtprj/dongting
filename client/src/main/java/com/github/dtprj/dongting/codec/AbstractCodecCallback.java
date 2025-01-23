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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.common.ByteArray;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public abstract class AbstractCodecCallback<T> {

    protected DecodeContext context;

    protected abstract T getResult();

    protected void begin(int len) {
    }

    protected boolean end(boolean success) {
        return success;
    }

    protected final StrEncoder parseStrEncoder(ByteBuffer buf, int fieldLen, int currentPos) {
        String s = parseUTF8(buf, fieldLen, currentPos);
        return s == null ? null : new StrEncoder(s);
    }

    protected final String parseUTF8(ByteBuffer buf, int fieldLen, int currentPos) {
        if (fieldLen == 0) {
            return "";
        }
        byte[] arr;
        ByteBuffer temp = null;
        int remain = buf.remaining();
        if (currentPos == 0) {
            if (remain >= fieldLen) {
                ByteBufferPool pool = null;
                if (fieldLen < DecodeContext.THREAD_LOCAL_BUFFER_SIZE) {
                    arr = context.getThreadLocalBuffer();
                } else {
                    pool = context.getHeapPool().getPool();
                    temp = pool.borrow(fieldLen);
                    arr = temp.array();
                }
                buf.get(arr, 0, fieldLen);
                String s = new String(arr, 0, fieldLen, StandardCharsets.UTF_8);
                if (pool != null) {
                    pool.release(temp);
                }
                return s;
            }
            if (fieldLen < 64) {
                arr = new byte[fieldLen];
                context.status = arr;
            } else {
                ByteBufferPool pool = context.getHeapPool().getPool();
                temp = pool.borrow(fieldLen);
                context.status = temp;
                arr = temp.array();
            }
        } else {
            if (fieldLen < 64) {
                arr = (byte[]) context.status;
            } else {
                temp = (ByteBuffer) context.status;
                arr = temp.array();
            }
        }
        int needRead = fieldLen - currentPos;
        if (remain < needRead) {
            buf.get(arr, currentPos, remain);
            return null;
        } else {
            buf.get(arr, currentPos, needRead);
            String s = new String(arr, 0, fieldLen, StandardCharsets.UTF_8);
            if (temp != null) {
                ByteBufferPool pool = context.getHeapPool().getPool();
                pool.release(temp);
            }
            return s;
        }
    }

    protected final ByteArray parseByteArray(ByteBuffer buf, int fieldLen, int currentPos) {
        byte[] arr = parseBytes(buf, fieldLen, currentPos);
        return arr == null ? null : new ByteArray(arr);
    }

    protected final byte[] parseBytes(ByteBuffer buf, int fieldLen, int currentPos) {
        if (fieldLen == 0) {
            return new byte[0];
        }
        byte[] result;
        if (currentPos == 0) {
            result = new byte[fieldLen];
            context.status = result;
        } else {
            result = (byte[]) context.status;
        }
        int remain = buf.remaining();
        int needRead = fieldLen - currentPos;
        if (remain < needRead) {
            buf.get(result, currentPos, remain);
            return null;
        } else {
            buf.get(result, currentPos, needRead);
            return result;
        }
    }

    protected final RefBuffer parseRefBuffer(ByteBuffer buf, int fieldLen, int currentPos) {
        if (fieldLen == 0) {
            return RefBuffer.wrap(SimpleByteBufferPool.EMPTY_BUFFER);
        }
        RefBuffer result;
        if (currentPos == 0) {
            result = context.getHeapPool().create(fieldLen);
            context.status = result;
        } else {
            result = (RefBuffer) context.status;
        }
        ByteBuffer resultBuf = result.getBuffer();
        resultBuf.put(buf);
        if (resultBuf.position() < fieldLen) {
            return null;
        } else {
            resultBuf.flip();
            return result;
        }
    }

    @SuppressWarnings("unchecked")
    protected final <X> X parseNested(ByteBuffer buf, int fieldLen, int currentPos,
                                      PbCallback<X> nestedCallback) {
        PbParser nestedParser = context.createOrGetNestedParser();
        if (currentPos == 0) {
            nestedParser.prepareNext(context.createOrGetNestedContext(), nestedCallback, fieldLen);
        }
        boolean end = buf.remaining() >= fieldLen - currentPos;
        X result = (X) nestedParser.parse(buf);
        if (end) {
            if (!nestedParser.isFinished()) {
                throw new PbException("parse not finish after read all bytes. fieldLen=" + fieldLen
                        + ", currentPos=" + currentPos + "class=" + getClass());
            }
        } else {
            if (nestedParser.isFinished()) {
                throw new PbException("parse finished without read all bytes. fieldLen=" + fieldLen
                        + ", currentPos=" + currentPos + "class=" + getClass());
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    protected final <X> X parseNested(ByteBuffer buf, int fieldLen, int currentPos,
                                      DecoderCallback<X> nestedCallback) {
        Decoder nestedDecoder = context.createOrGetNestedDecoder();
        if (currentPos == 0) {
            nestedDecoder.prepareNext(context.createOrGetNestedContext(), nestedCallback);
        }
        boolean end = buf.remaining() >= fieldLen - currentPos;
        X result = (X) nestedDecoder.decode(buf, fieldLen, currentPos);
        if (end) {
            if (!nestedDecoder.isFinished()) {
                throw new PbException("decode not finish after read all bytes. fieldLen="
                        + fieldLen + ", currentPos=" + currentPos + "class=" + getClass());
            }
        } else {
            if (nestedDecoder.isFinished()) {
                throw new PbException("decode finished without read all bytes. fieldLen="
                        + fieldLen + ", currentPos=" + currentPos + "class=" + getClass());
            }
        }
        return result;
    }

}

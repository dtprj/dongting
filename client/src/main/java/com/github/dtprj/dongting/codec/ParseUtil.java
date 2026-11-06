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

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author huangli
 */
public final class ParseUtil {

    private static final DtLog log = DtLogs.getLogger(ParseUtil.class);

    public static StrEncoder parseStrEncoder(DecodeContext context, ByteBuffer buf, int fieldLen, int currentPos) {
        String s = parseUTF8(context, buf, fieldLen, currentPos);
        return s == null ? null : new StrEncoder(s);
    }

    public static String parseUTF8(DecodeContext context, ByteBuffer buf, int fieldLen, int currentPos) {
        if (fieldLen == 0) {
            return "";
        }
        if (currentPos == 0 && buf.remaining() >= fieldLen) {
            return StrDecoderCallback.decodeSingleBuffer(buf, fieldLen, context);
        }
        return parseNested(context, buf, fieldLen, currentPos, context.strDecoderCallback());
    }

    public static ByteArray parseByteArray(DecodeContext context, ByteBuffer buf, int fieldLen, int currentPos) {
        byte[] arr = parseBytes(context, buf, fieldLen, currentPos);
        return arr == null ? null : new ByteArray(arr);
    }

    public static byte[] parseBytes(DecodeContext context, ByteBuffer buf, int fieldLen, int currentPos) {
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

    @SuppressWarnings("unchecked")
    public static <X> X parseNested(DecodeContext context, ByteBuffer buf, int fieldLen, int currentPos,
                                      PbCallback<X> nestedCallback) {
        PbParser nestedParser = context.createOrGetNestedParser();
        DecodeContext nestedContext = context.createOrGetNestedContext();
        if (currentPos == 0) {
            nestedParser.prepareNext(nestedContext, nestedCallback, fieldLen);
        }
        boolean end = buf.remaining() >= fieldLen - currentPos;
        X result = (X) nestedParser.parse(buf);
        if (end) {
            if (!nestedParser.isFinished()) {
                throw new PbException("parse not finish after read all bytes. fieldLen=" + fieldLen
                        + ", currentPos=" + currentPos
                        + ", class=" + nestedCallback.getClass().getName());
            }
            nestedContext.reset(nestedParser);
        } else {
            if (nestedParser.isFinished()) {
                throw new PbException("parse finished without read all bytes. fieldLen=" + fieldLen
                        + ", currentPos=" + currentPos
                        + ", class=" + nestedCallback.getClass().getName());
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <X> X parseNested(DecodeContext context, ByteBuffer buf, int fieldLen, int currentPos,
                                      DecoderCallback<X> nestedCallback) {
        Decoder nestedDecoder = context.createOrGetNestedDecoder();
        DecodeContext nestedContext = context.createOrGetNestedContext();
        if (currentPos == 0) {
            nestedDecoder.prepareNext(nestedContext, nestedCallback);
        }
        boolean end = buf.remaining() >= fieldLen - currentPos;
        X result = (X) nestedDecoder.decode(buf, fieldLen, currentPos);
        if (end) {
            if (!nestedDecoder.isFinished()) {
                throw new PbException("decode not finish after read all bytes. fieldLen="
                        + fieldLen + ", currentPos=" + currentPos
                        + ", class=" + nestedCallback.getClass().getName());
            }
            nestedContext.reset(nestedDecoder);
        } else {
            if (nestedDecoder.isFinished()) {
                throw new PbException("decode finished without read all bytes. fieldLen="
                        + fieldLen + ", currentPos=" + currentPos
                        + ", class=" + nestedCallback.getClass().getName());
            }
        }
        return result;
    }

    private static final int MAX_SUGGESTED_SIZE = 50000;

    /** Returns the suggested size if it is in (0, 50000], otherwise 0. */
    public static int checkSuggestedSize(int suggestedSize) {
        if (suggestedSize <= 0 || suggestedSize > MAX_SUGGESTED_SIZE) {
            log.warn("invalid suggested size: {}", suggestedSize);
            return 0;
        }
        return suggestedSize;
    }

    public static <X> ArrayList<X> createArrayList(int suggestedSize) {
        return new ArrayList<>(checkSuggestedSize(suggestedSize));
    }

}

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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.common.DtThread;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Decode a UTF-8 string. The single-buffer case is handled by {@link #decodeSingleBuffer};
 * the multi-buffer case keeps the borrowed {@link RefBuffer} in {@link #tempRef} and releases
 * it in {@link #end(boolean)}, which the {@link Decoder} framework always invokes, so no
 * buffer leaks even if parsing is cancelled mid-field.
 *
 * @author huangli
 */
public class StrDecoderCallback extends DecoderCallback<String> {

    private RefBuffer tempRef;
    private byte[] smallArr;
    private String s;

    @Override
    public void doDecode(ByteBuffer buf, int fieldLen, int currentPos) {
        if (fieldLen == 0) {
            s = "";
            return;
        }
        int remain = buf.remaining();

        // single buffer fast path
        if (currentPos == 0 && remain >= fieldLen) {
            s = decodeSingleBuffer(buf, fieldLen, context);
            return;
        }

        // multi-buffer accumulation path
        byte[] arr;
        int off;
        if (currentPos == 0) {
            if (fieldLen < 64) {
                smallArr = new byte[fieldLen];
                arr = smallArr;
                off = 0;
            } else {
                tempRef = context.buffers.borrowRefBuffer(fieldLen, true, false, 0);
                ByteBuffer destBuf = tempRef.getBuffer();
                arr = destBuf.array();
                off = destBuf.arrayOffset();
            }
        } else {
            if (fieldLen < 64) {
                arr = smallArr;
                off = 0;
            } else {
                ByteBuffer destBuf = tempRef.getBuffer();
                arr = destBuf.array();
                off = destBuf.arrayOffset();
            }
        }
        int needRead = fieldLen - currentPos;
        if (remain < needRead) {
            buf.get(arr, off + currentPos, remain);
        } else {
            buf.get(arr, off + currentPos, needRead);
            s = new String(arr, off, fieldLen, StandardCharsets.UTF_8);
            if (tempRef != null) {
                tempRef.release();
                tempRef = null;
            }
        }
    }

    /**
     * Single-buffer fast path, shared by {@link #doDecode} and
     * {@link AbstractCodecCallback#parseUTF8} to avoid nested-decoder overhead.
     */
    static String decodeSingleBuffer(ByteBuffer buf, int fieldLen, DecodeContext context) {
        byte[] arr;
        int off;
        RefBuffer tempRef = null;
        try {
            if (fieldLen < DtThread.THREAD_LOCAL_BUFFER_SIZE) {
                arr = context.threadLocalBuffer;
                off = 0;
            } else {
                tempRef = context.buffers.borrowRefBuffer(fieldLen, true, false, 0);
                ByteBuffer destBuf = tempRef.getBuffer();
                arr = destBuf.array();
                off = destBuf.arrayOffset();
            }
            buf.get(arr, off, fieldLen);
            return new String(arr, off, fieldLen, StandardCharsets.UTF_8);
        } finally {
            if (tempRef != null) {
                tempRef.release();
            }
        }
    }

    @Override
    protected String getResult() {
        return s;
    }

    @Override
    protected void end(boolean success) {
        if (tempRef != null) {
            tempRef.release();
            tempRef = null;
        }
        smallArr = null;
        s = null;
    }
}

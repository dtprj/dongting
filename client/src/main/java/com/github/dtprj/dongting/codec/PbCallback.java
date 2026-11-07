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

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public interface PbCallback<T> {

    T getResult(DecodeContext context);

    default void begin(int len) {
    }

    /**
     * @param success whether the parse/decode completed successfully
     * @param ex      the exception that caused the failure; null if success is true, or if the
     *                parse/decode was cancelled (e.g. by reset) without a specific exception.
     *                It may be any exception thrown during parse/decode (e.g. CodecException),
     *                not necessarily a NetException
     */
    default void end(boolean success, Throwable ex) {
    }

    default void readVarNumber(DecodeContext context, int index, long value) {
    }

    default void readFix32(DecodeContext context, int index, int value) {
    }

    default void readFix64(DecodeContext context, int index, long value) {
    }

    /**
     * Consume the whole chunk for recognized indexes (buf limit is the chunk end), or leave the
     * first chunk untouched to skip an unknown field; partial consumption fails the parse.
     */
    default void readBytes(DecodeContext context, int index, ByteBuffer buf, int fieldLen, int currentPos) {
        buf.position(buf.limit());
    }
}

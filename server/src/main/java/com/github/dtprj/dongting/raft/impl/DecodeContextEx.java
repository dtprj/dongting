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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.buf.Buffers;
import com.github.dtprj.dongting.codec.DecodeContext;

import java.util.zip.CRC32C;

/**
 * Decode context used on server side, where Java 11 and server-only classes are available.
 *
 * @author huangli
 */
public final class DecodeContextEx extends DecodeContext {

    public final CRC32C crc;

    public DecodeContextEx(Buffers buffers, byte[] threadLocalBuffer) {
        this(buffers, threadLocalBuffer, new CRC32C());
    }

    private DecodeContextEx(Buffers buffers, byte[] threadLocalBuffer, CRC32C crc) {
        super(buffers, threadLocalBuffer);
        this.crc = crc;
    }

    @Override
    protected DecodeContextEx createNestedInstance() {
        // outer and nested contexts share one crc instance, they never use it at the same time
        return new DecodeContextEx(buffers, threadLocalBuffer, crc);
    }
}

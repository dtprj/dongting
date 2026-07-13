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
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.net.WritePacket;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public abstract class PreEncodedWritePacket extends WritePacket {

    protected int totalPreEncodedSize;
    protected ByteBuffer preEncodedBuffer;

    @Override
    public boolean hasPreEncodedBuffer() {
        return preEncodedBuffer != null;
    }

    @Override
    public int getTotalPreEncodedBufferSize() {
        return totalPreEncodedSize;
    }

    @Override
    public ByteBuffer getPreEncodedBuffer() {
        ByteBuffer buf = preEncodedBuffer;
        preEncodedBuffer = null;
        return buf;
    }
}

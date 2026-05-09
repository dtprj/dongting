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
package com.github.dtprj.dongting.raft.store;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class SingleBufferCallback implements IoCallback {
    private final ByteBuffer buffer;
    private final long filePos;
    private final boolean write;
    private final int oldBufferPos;

    public SingleBufferCallback(ByteBuffer buffer, long filePos, boolean write) {
        this.buffer = buffer;
        this.filePos = filePos;
        this.oldBufferPos = buffer.position();
        this.write = write;
    }

    @Override
    public void run(ByteBuffer mmapBuffer) {
        buffer.position(oldBufferPos);
        mmapBuffer.position((int) filePos);
        if (write) {
            mmapBuffer.put(buffer);
        } else {
            mmapBuffer.limit((int) filePos + buffer.remaining());
            buffer.put(mmapBuffer);
        }
    }
}

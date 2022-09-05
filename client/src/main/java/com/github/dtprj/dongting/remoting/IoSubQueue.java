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
package com.github.dtprj.dongting.remoting;

import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * @author huangli
 */
class IoSubQueue {
    private final Runnable registerForWrite;
    private ByteBuffer writeBuffer;

    private final LinkedList<ByteBuffer> subQueue = new LinkedList<>();
    private int subQueueBytes;
    private boolean writing;

    public IoSubQueue(Runnable registerForWrite) {
        this.registerForWrite = registerForWrite;
    }

    public void enqueue(ByteBuffer buf) {
        LinkedList<ByteBuffer> subQueue = this.subQueue;
        subQueue.add(buf);
        subQueueBytes += buf.remaining();
        if (subQueue.size() == 1 && !writing) {
            registerForWrite.run();
        }
    }

    //TODO need optimise
    public ByteBuffer getWriteBuffer() {
        ByteBuffer writeBuffer = this.writeBuffer;
        if (writeBuffer != null && writeBuffer.remaining() > 0) {
            return writeBuffer;
        }
        ByteBuffer buf = ByteBuffer.allocateDirect(subQueueBytes);
        LinkedList<ByteBuffer> subQueue = this.subQueue;
        for(ByteBuffer frame: subQueue) {
            buf.put(frame);
        }
        subQueue.clear();
        buf.flip();
        subQueueBytes = 0;
        this.writeBuffer = buf;
        return buf;
    }

    public void setWriting(boolean writing) {
        this.writing = writing;
    }
}

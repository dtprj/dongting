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
import java.util.ArrayList;

/**
 * @author huangli
 */
class IoSubQueue {
    private final Runnable registerForWrite;
    private final ByteBufferPool pool;
    private ByteBuffer writeBuffer;

    private final ArrayList<WriteFrame> subQueue = new ArrayList<>();
    private int subQueueBytes;
    private boolean writing;

    public IoSubQueue(Runnable registerForWrite, ByteBufferPool pool) {
        this.registerForWrite = registerForWrite;
        this.pool = pool;
    }

    public void enqueue(WriteFrame frame) {
        ArrayList<WriteFrame> subQueue = this.subQueue;
        subQueue.add(frame);
        subQueueBytes += frame.estimateSize();
        if (subQueue.size() == 1 && !writing) {
            registerForWrite.run();
        }
    }

    public ByteBuffer getWriteBuffer(long nanoTime) {
        ByteBuffer writeBuffer = this.writeBuffer;
        if (writeBuffer != null) {
            if (writeBuffer.remaining() > 0) {
                return writeBuffer;
            } else {
                pool.release(writeBuffer, nanoTime);
                this.writeBuffer = null;
            }
        }
        if (subQueueBytes == 0) {
            return null;
        }
        ByteBuffer buf = pool.borrow(subQueueBytes);
        ArrayList<WriteFrame> subQueue = this.subQueue;
        int size = subQueue.size();
        for (int i = 0; i < size; i++) {
            subQueue.get(i).encode(buf);
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

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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.buf.ByteBufferPool;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;

/**
 * @author huangli
 */
class IoSubQueue {
    private static final int MAX_BUFFER_SIZE = 512 * 1024;
    private final ByteBufferPool directPool;
    private final ByteBufferPool heapPool;
    private Runnable registerForWrite;
    private ByteBuffer writeBuffer;

    private final ArrayDeque<WriteFrame> subQueue = new ArrayDeque<>();
    private int subQueueBytes;
    private boolean writing;

    public IoSubQueue(ByteBufferPool directPool, ByteBufferPool heapPool) {
        this.directPool = directPool;
        this.heapPool = heapPool;
    }

    public void setRegisterForWrite(Runnable registerForWrite) {
        this.registerForWrite = registerForWrite;
    }

    public void enqueue(WriteFrame frame) {
        ArrayDeque<WriteFrame> subQueue = this.subQueue;
        subQueue.addLast(frame);
        subQueueBytes += frame.estimateSize();
        if (subQueue.size() == 1 && !writing) {
            registerForWrite.run();
        }
    }

    public ByteBuffer getWriteBuffer() {
        ByteBuffer writeBuffer = this.writeBuffer;
        if (writeBuffer != null) {
            if (writeBuffer.remaining() > 0) {
                return writeBuffer;
            } else {
                directPool.release(writeBuffer);
                this.writeBuffer = null;
            }
        }
        int subQueueBytes = this.subQueueBytes;
        if (subQueueBytes == 0) {
            return null;
        }
        ArrayDeque<WriteFrame> subQueue = this.subQueue;
        ByteBuffer buf = null;
        if (subQueueBytes <= MAX_BUFFER_SIZE) {
            buf = directPool.borrow(subQueueBytes);
            WriteFrame f;
            while ((f = subQueue.pollFirst()) != null) {
                f.encode(buf, heapPool);
            }
            this.subQueueBytes = 0;
        } else {
            WriteFrame f;
            while ((f = subQueue.pollFirst()) != null) {
                int size = f.estimateSize();
                if (buf == null) {
                    if (size > MAX_BUFFER_SIZE) {
                        buf = directPool.borrow(size);
                        f.encode(buf, heapPool);
                        subQueueBytes -= size;
                        break;
                    } else {
                        buf = directPool.borrow(MAX_BUFFER_SIZE);
                    }
                }
                if (size > buf.remaining()) {
                    subQueue.addFirst(f);
                    break;
                } else {
                    f.encode(buf, heapPool);
                    subQueueBytes -= size;
                }
            }
            this.subQueueBytes = subQueueBytes;
        }
        assert buf != null;
        buf.flip();
        this.writeBuffer = buf;
        return buf;
    }

    public void setWriting(boolean writing) {
        this.writing = writing;
    }
}

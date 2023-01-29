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
import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class IoSubQueue {
    private static final DtLog log = DtLogs.getLogger(IoSubQueue.class);

    private static final int MAX_BUFFER_SIZE = 512 * 1024;
    private final ByteBufferPool directPool;
    private final ByteBufferPool heapPool;
    private final WorkerStatus workerStatus;
    private final NioStatus nioStatus;
    private final DtChannel dtc;
    private Runnable registerForWrite;
    private ByteBuffer writeBuffer;

    private final ArrayDeque<WriteData> subQueue = new ArrayDeque<>();
    private int subQueueBytes;
    private boolean writing;

    public IoSubQueue(WorkerStatus workerStatus, NioStatus nioStatus, DtChannel dtc) {
        this.directPool = workerStatus.getDirectPool();
        this.heapPool = workerStatus.getHeapPool();
        this.workerStatus = workerStatus;
        this.nioStatus = nioStatus;
        this.dtc = dtc;
    }

    public void setRegisterForWrite(Runnable registerForWrite) {
        this.registerForWrite = registerForWrite;
    }

    public void enqueue(WriteData writeData) {
        ArrayDeque<WriteData> subQueue = this.subQueue;
        subQueue.addLast(writeData);
        subQueueBytes += writeData.getData().estimateSize();
        if (subQueue.size() == 1 && !writing) {
            registerForWrite.run();
        }
    }

    public ByteBuffer getWriteBuffer(Timestamp roundTime) {
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
        ArrayDeque<WriteData> subQueue = this.subQueue;
        ByteBuffer buf = null;
        if (subQueueBytes <= MAX_BUFFER_SIZE) {
            buf = directPool.borrow(subQueueBytes);
            WriteData wd;
            while ((wd = subQueue.pollFirst()) != null) {
                if (checkTimeoutAndAddToPending(wd, roundTime)) {
                    wd.getData().encode(buf, heapPool);
                }
            }
            this.subQueueBytes = 0;
        } else {
            WriteData wd;
            while ((wd = subQueue.pollFirst()) != null) {
                if(!checkTimeoutAndAddToPending(wd, roundTime)){
                    continue;
                }
                WriteFrame f = wd.getData();
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
                    subQueue.addFirst(wd);
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
        if (buf.remaining() == 0) {
            directPool.release(buf);
            return null;
        }
        this.writeBuffer = buf;
        return buf;
    }

    private boolean checkTimeoutAndAddToPending(WriteData wd, Timestamp roundTime) {
        WriteFrame f = wd.getData();
        DtTime t = wd.getTimeout();
        long rest = t.rest(TimeUnit.NANOSECONDS, roundTime);
        if (rest < 0) {
            String frameType;
            if (f.getFrameType() == FrameType.TYPE_RESP) {
                frameType = "response";
            } else {
                frameType = "request";
                nioStatus.getRequestSemaphore().release();
                String msg = "timeout before send: " + t.getTimeout(TimeUnit.MILLISECONDS) + "ms";
                wd.getFuture().completeExceptionally(new NetTimeoutException(msg));
            }
            log.debug("{} timeout before send: {}ms, seq={}, {}", frameType,
                    t.getTimeout(TimeUnit.MILLISECONDS), wd.getData().getSeq(),
                    wd.getDtc());
            return false;
        }

        if (f.getFrameType() == FrameType.TYPE_RESP) {
            return true;
        }

        int seq = dtc.getAndIncSeq();
        f.setSeq(seq);
        f.setTimeout(rest);
        long key = BitUtil.toLong(dtc.getChannelIndexInWorker(), seq);
        WriteData old = workerStatus.getPendingRequests().put(key, wd);
        if (old != null) {
            nioStatus.getRequestSemaphore().release();
            String errMsg = "dup seq: old=" + old.getData() + ", new=" + f;
            log.error(errMsg);
            wd.getFuture().completeExceptionally(new NetException(errMsg));
            workerStatus.getPendingRequests().put(key, old);
            return false;
        }
        return true;
    }

    public void setWriting(boolean writing) {
        this.writing = writing;
    }
}

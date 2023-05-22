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
    private final DtChannel dtc;
    private Runnable registerForWrite;

    private ByteBuffer writeBuffer;
    private int packetOfBuffer;

    private final ArrayDeque<WriteData> subQueue = new ArrayDeque<>();
    private int subQueueBytes;
    private boolean writing;


    public IoSubQueue(WorkerStatus workerStatus, DtChannel dtc) {
        this.directPool = workerStatus.getDirectPool();
        this.heapPool = workerStatus.getHeapPool();
        this.workerStatus = workerStatus;
        this.dtc = dtc;
    }

    public void setRegisterForWrite(Runnable registerForWrite) {
        this.registerForWrite = registerForWrite;
    }

    public void enqueue(WriteData writeData) {
        ArrayDeque<WriteData> subQueue = this.subQueue;
        subQueue.addLast(writeData);
        subQueueBytes += writeData.getData().actualSize();
        if (subQueue.size() == 1 && !writing) {
            registerForWrite.run();
        }
        workerStatus.setPacketToWrite(workerStatus.getPacketToWrite() + 1);
    }

    public void cleanSubQueue() {
        WriteData wd;
        while ((wd = subQueue.pollFirst()) != null) {
            if (wd.getFuture() != null) {
                wd.getFuture().completeExceptionally(new NetException("channel closed, future cancelled by subQueue clean"));
            }
        }
    }

    public ByteBuffer getWriteBuffer(Timestamp roundTime) {
        ByteBuffer writeBuffer = this.writeBuffer;
        if (writeBuffer != null) {
            if (writeBuffer.remaining() > 0) {
                return writeBuffer;
            } else {
                // current buffer write finished
                workerStatus.setPacketToWrite(workerStatus.getPacketToWrite() - packetOfBuffer);
                directPool.release(writeBuffer);
                this.writeBuffer = null;
                packetOfBuffer = 0;
            }
        }
        int subQueueBytes = this.subQueueBytes;
        if (subQueueBytes == 0) {
            // no packet to write
            return null;
        }
        ArrayDeque<WriteData> subQueue = this.subQueue;
        ByteBuffer buf = null;
        if (subQueueBytes <= MAX_BUFFER_SIZE) {
            buf = directPool.borrow(subQueueBytes);
            WriteData wd;
            while ((wd = subQueue.pollFirst()) != null) {
                encode(buf, wd, roundTime);
            }
            this.subQueueBytes = 0;
        } else {
            WriteData wd;
            while ((wd = subQueue.pollFirst()) != null) {
                WriteFrame f = wd.getData();
                int size = f.actualSize();
                if (buf == null) {
                    if (size > MAX_BUFFER_SIZE) {
                        buf = directPool.borrow(size);
                        encode(buf, wd, roundTime);
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
                    encode(buf, wd, roundTime);
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

    private void encode(ByteBuffer buf, WriteData wd, Timestamp roundTime) {
        WriteFrame f = wd.getData();
        boolean request = f.getFrameType() == FrameType.TYPE_REQ;
        DtTime t = wd.getTimeout();
        long rest = t.rest(TimeUnit.NANOSECONDS, roundTime);
        if (rest > 0) {
            if (request) {
                int seq = dtc.getAndIncSeq();
                f.setSeq(seq);
                f.setTimeout(rest);
            }
            try {
                f.encode(buf, heapPool);
            } catch (RuntimeException | Error e) {
                if (wd.getFuture() != null) {
                    wd.getFuture().completeExceptionally(e);
                }
                workerStatus.setPacketToWrite(workerStatus.getPacketToWrite() - 1);
                throw e;
            }
            packetOfBuffer++;
            if (!request) {
                return;
            }
            long key = BitUtil.toLong(dtc.getChannelIndexInWorker(), f.getSeq());
            WriteData old = workerStatus.getPendingRequests().put(key, wd);
            if (old != null) {
                String errMsg = "dup seq: old=" + old.getData() + ", new=" + f;
                log.error(errMsg);
                wd.getFuture().completeExceptionally(new NetException(errMsg));
                workerStatus.getPendingRequests().put(key, old);
            }
        } else {
            if (request) {
                String msg = "timeout before send: " + t.getTimeout(TimeUnit.MILLISECONDS) + "ms";
                wd.getFuture().completeExceptionally(new NetTimeoutException(msg));
                log.info("request timeout before send: {}ms, channel={}",
                        t.getTimeout(TimeUnit.MILLISECONDS), wd.getDtc().getChannel());
            } else {
                log.info("request timeout before send: {}ms, seq={}, channel={}",
                        t.getTimeout(TimeUnit.MILLISECONDS), f.getSeq(), wd.getDtc().getChannel());
            }
            workerStatus.setPacketToWrite(workerStatus.getPacketToWrite() - 1);
        }
    }

    public void setWriting(boolean writing) {
        this.writing = writing;
    }
}

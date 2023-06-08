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
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.codec.EncodeContext;
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
    private int framesInBuffer;

    private final ArrayDeque<WriteData> subQueue = new ArrayDeque<>();
    private int subQueueBytes;
    private boolean writing;

    private WriteData lastWriteData;
    private final EncodeContext encodeContext;

    public IoSubQueue(WorkerStatus workerStatus, DtChannel dtc, RefBufferFactory heapPool) {
        this.directPool = workerStatus.getDirectPool();
        this.heapPool = workerStatus.getHeapPool();
        this.workerStatus = workerStatus;
        this.dtc = dtc;
        this.encodeContext = new EncodeContext(heapPool);
    }

    public void setRegisterForWrite(Runnable registerForWrite) {
        this.registerForWrite = registerForWrite;
    }

    public void enqueue(WriteData writeData) {
        WriteFrame wf = writeData.getData();
        int estimateSize = wf.calcMaxFrameSize();
        if (estimateSize < 0) {
            wf.clean(encodeContext);
            fail(writeData, "estimateSize overflow");
            return;
        }
        writeData.setEstimateSize(estimateSize);

        ArrayDeque<WriteData> subQueue = this.subQueue;
        subQueue.addLast(writeData);

        // the subQueueBytes is not accurate
        subQueueBytes += estimateSize;
        if (subQueue.size() == 1 && !writing) {
            registerForWrite.run();
        }
        workerStatus.setFramesToWrite(workerStatus.getFramesToWrite() + 1);
    }

    private void fail(WriteData writeData, String msg) {
        if (writeData.getFuture() != null) {
            writeData.getFuture().completeExceptionally(new NetException(msg));
        }
    }

    public void cleanSubQueue() {
        WriteData wd;
        while ((wd = subQueue.pollFirst()) != null) {
            fail(wd, "channel closed, future cancelled by subQueue clean");
            wd.getData().clean(encodeContext);
        }
    }

    public ByteBuffer getWriteBuffer(Timestamp roundTime) {
        ByteBuffer writeBuffer = this.writeBuffer;
        if (writeBuffer != null) {
            if (writeBuffer.remaining() > 0) {
                return writeBuffer;
            } else {
                // current buffer write finished
                workerStatus.setFramesToWrite(workerStatus.getFramesToWrite() - framesInBuffer);
                directPool.release(writeBuffer);
                this.writeBuffer = null;
                framesInBuffer = 0;
            }
        }
        int subQueueBytes = this.subQueueBytes;
        ArrayDeque<WriteData> subQueue = this.subQueue;
        if (subQueue.size() == 0 && lastWriteData == null) {
            // no packet to write
            return null;
        }
        ByteBuffer buf = subQueueBytes <= MAX_BUFFER_SIZE ? heapPool.borrow(subQueueBytes) : heapPool.borrow(MAX_BUFFER_SIZE);

        WriteData wd = this.lastWriteData;
        try {
            while (subQueue.size() > 0 || wd != null) {
                boolean encodeFinish;
                if (wd == null) {
                    wd = subQueue.pollFirst();
                    encodeFinish = encode(buf, wd, roundTime);
                } else {
                    encodeFinish = doEncode(buf, wd);
                }
                if (encodeFinish) {
                    wd.getData().clean(encodeContext);
                    subQueueBytes -= wd.getEstimateSize();
                    if (subQueueBytes < 0) {
                        subQueueBytes = 0;
                    }
                    WriteFrame f = wd.getData();
                    if (f.getFrameType() == FrameType.TYPE_REQ) {
                        long key = BitUtil.toLong(dtc.getChannelIndexInWorker(), f.getSeq());
                        WriteData old = workerStatus.getPendingRequests().put(key, wd);
                        if (old != null) {
                            // TODO change this behavior
                            String errMsg = "dup seq: old=" + old.getData() + ", new=" + f;
                            log.error(errMsg);
                            fail(wd, errMsg);
                            workerStatus.getPendingRequests().put(key, old);
                        }
                    }
                    encodeContext.setStatus(null);
                    wd = null;
                } else {
                    return flipAndReturnBuffer(buf);
                }
            }
            subQueueBytes = 0;
            return flipAndReturnBuffer(buf);
        } finally {
            this.lastWriteData = wd;
            this.subQueueBytes = subQueueBytes;
        }
    }

    private ByteBuffer flipAndReturnBuffer(ByteBuffer buf) {
        buf.flip();
        if (buf.remaining() == 0) {
            directPool.release(buf);
            this.writeBuffer = null;
            return null;
        } else {
            this.writeBuffer = buf;
            return buf;
        }
    }

    private boolean encode(ByteBuffer buf, WriteData wd, Timestamp roundTime) {
        WriteFrame f = wd.getData();
        boolean request = f.getFrameType() == FrameType.TYPE_REQ;
        DtTime t = wd.getTimeout();
        long rest = t.rest(TimeUnit.NANOSECONDS, roundTime);
        if (rest <= 0) {
            if (request) {
                String msg = "timeout before send: " + t.getTimeout(TimeUnit.MILLISECONDS) + "ms";
                log.info("request timeout before send: {}ms, channel={}",
                        t.getTimeout(TimeUnit.MILLISECONDS), wd.getDtc().getChannel());
                if (wd.getFuture() != null) {
                    wd.getFuture().completeExceptionally(new NetTimeoutException(msg));
                }
            } else {
                log.info("response timeout before send: {}ms, seq={}, channel={}",
                        t.getTimeout(TimeUnit.MILLISECONDS), f.getSeq(), wd.getDtc().getChannel());
            }
            workerStatus.setFramesToWrite(workerStatus.getFramesToWrite() - 1);
            return true;
        }
        framesInBuffer++;

        if (request) {
            int seq = dtc.getAndIncSeq();
            f.setSeq(seq);
            f.setTimeout(rest);
        }
        encodeContext.setStatus(null);
        return doEncode(buf, wd);
    }

    private boolean doEncode(ByteBuffer buf, WriteData wd) {
        try {
            WriteFrame wf = wd.getData();
            return wf.encode(encodeContext, buf, wf);
        } catch (RuntimeException | Error e) {
            if (wd.getFuture() != null) {
                wd.getFuture().completeExceptionally(e);
            }
            wd.getData().clean(encodeContext);
            encodeContext.setStatus(null);
            // channel will be closed
            throw e;
        }
    }

    public void setWriting(boolean writing) {
        this.writing = writing;
    }
}

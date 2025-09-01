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
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class IoChannelQueue {
    private static final DtLog log = DtLogs.getLogger(IoChannelQueue.class);

    private static final int ENCODE_NOT_FINISH = 1;
    private static final int ENCODE_FINISH = 2;
    private static final int ENCODE_CANCEL = 3;

    private static final int MAX_BUFFER_SIZE = 512 * 1024;
    private final ByteBufferPool directPool;
    private final WorkerStatus workerStatus;
    private final DtChannelImpl dtc;
    private Runnable registerForWrite;

    private ByteBuffer writeBuffer;
    private int packetsInBuffer;

    private final ArrayDeque<WriteData> subQueue = new ArrayDeque<>();
    private int subQueueBytes;
    private boolean writing;

    private WriteData lastWriteData;
    private final EncodeContext encodeContext;

    private final PerfCallback perfCallback;

    public IoChannelQueue(NioConfig config, WorkerStatus workerStatus, DtChannelImpl dtc, RefBufferFactory heapPool) {
        this.directPool = workerStatus.directPool;
        this.workerStatus = workerStatus;
        this.dtc = dtc;
        this.encodeContext = new EncodeContext(heapPool);
        this.perfCallback = config.perfCallback;
    }

    public void setRegisterForWrite(Runnable registerForWrite) {
        this.registerForWrite = registerForWrite;
    }

    public void enqueue(WriteData writeData) {
        WritePacket wf = writeData.getData();
        if (wf.use) {
            writeData.callFail(true, new NetException("WritePacket is used"));
            return;
        }
        wf.use = true;

        writeData.perfTime = perfCallback.takeTime(PerfConsts.RPC_D_CHANNEL_QUEUE);
        subQueue.addLast(writeData);

        // the subQueueBytes is not accurate
        // can't invoke actualSize() here because seq and timeout field is not set yet
        subQueueBytes += writeData.estimateSize;
        if (subQueue.size() == 1 && !writing) {
            registerForWrite.run();
        }
        workerStatus.addPacketsToWrite(1);
    }

    public void cleanChannelQueue() {
        if (packetsInBuffer > 0) {
            workerStatus.addPacketsToWrite(-packetsInBuffer);
        }
        if (this.writeBuffer != null) {
            directPool.release(this.writeBuffer);
            this.writeBuffer = null;
        }

        if (lastWriteData != null) {
            workerStatus.addPacketsToWrite(-1);
            lastWriteData.callFail(true, new NetException("channel closed, cancel request still in IoChannelQueue. 1"));
        }
        WriteData wd;
        while ((wd = subQueue.pollFirst()) != null) {
            wd.callFail(true, new NetException("channel closed, cancel request still in IoChannelQueue. 2"));
            workerStatus.addPacketsToWrite(-1);
        }
    }

    void afterBufferWriteFinish(){
        // current buffer write finished
        workerStatus.addPacketsToWrite(-packetsInBuffer);
        directPool.release(writeBuffer);
        this.writeBuffer = null;
        packetsInBuffer = 0;
    }

    public ByteBuffer getWriteBuffer(Timestamp roundTime) {
        ByteBuffer writeBuffer = this.writeBuffer;
        if (writeBuffer != null) {
            if (writeBuffer.remaining() > 0) {
                return writeBuffer;
            }
        }
        int subQueueBytes = this.subQueueBytes;
        ArrayDeque<WriteData> subQueue = this.subQueue;
        if (subQueue.isEmpty() && lastWriteData == null) {
            // no packet to write
            return null;
        }
        ByteBuffer buf = subQueueBytes <= MAX_BUFFER_SIZE ? directPool.borrow(subQueueBytes) : directPool.borrow(MAX_BUFFER_SIZE);

        WriteData wd = this.lastWriteData;
        try {
            while (!subQueue.isEmpty() || wd != null) {
                int encodeResult;
                if (wd == null) {
                    wd = subQueue.pollFirst();
                    perfCallback.fireTime(PerfConsts.RPC_D_CHANNEL_QUEUE, wd.perfTime);
                    encodeResult = encode(buf, wd, roundTime);
                } else {
                    encodeResult = doEncode(buf, wd);
                }
                if (encodeResult == ENCODE_NOT_FINISH) {
                    if (buf.position() == 0) {
                        workerStatus.addPacketsToWrite(-1);
                        subQueueBytes = Math.max(0, subQueueBytes - wd.estimateSize);
                        encodeContext.reset();
                        Throwable ex = new NetException("encode fail when buffer is empty");
                        wd.callFail(true, ex);
                        wd = null;
                        BugLog.log(ex);
                        continue;
                    }
                    return flipAndReturnBuffer(buf);
                } else {
                    if (encodeResult == ENCODE_FINISH) {
                        WritePacket f = wd.getData();
                        if (f.packetType == PacketType.TYPE_REQ) {
                            long key = BitUtil.toLong(dtc.getChannelIndexInWorker(), f.seq);
                            WriteData old = workerStatus.pendingRequests.put(key, wd);
                            if (old != null) {
                                String errMsg = "dup seq: old=" + old.getData() + ", new=" + f;
                                BugLog.getLog().error(errMsg);
                                old.callFail(true, new NetException(errMsg));
                            }
                        }
                        packetsInBuffer++;
                        if (f.packetType == PacketType.TYPE_ONE_WAY) {
                            // TODO complete after write finished
                            wd.callSuccess(null);
                        }
                    } else {
                        // cancel
                        workerStatus.addPacketsToWrite(-1);
                        String msg = "timeout before send: " + wd.getTimeout().getTimeout(TimeUnit.MILLISECONDS) + "ms";
                        wd.callFail(false, new NetTimeoutException(msg));
                    }

                    subQueueBytes = Math.max(0, subQueueBytes - wd.estimateSize);
                    try {
                        wd.getData().clean();
                    } finally {
                        encodeContext.reset();
                        wd = null;
                    }
                }
            }
            subQueueBytes = 0;
            return flipAndReturnBuffer(buf);
        } catch (RuntimeException | Error e) {
            encodeContext.reset();
            // channel will be closed, and cleanChannelQueue will be called
            throw e;
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

    private int encode(ByteBuffer buf, WriteData wd, Timestamp roundTime) {
        WritePacket f = wd.getData();
        // request or one way request
        boolean request = f.packetType != PacketType.TYPE_RESP;
        DtTime t = wd.getTimeout();
        long rest = t.rest(TimeUnit.NANOSECONDS, roundTime);
        if (rest <= 0) {
            if (request) {
                log.warn("request timeout before send: {}ms, cmd={}, seq={}, channel={}",
                        t.getTimeout(TimeUnit.MILLISECONDS), f.command, f.seq, wd.getDtc().getChannel());
            } else {
                log.warn("response timeout before send: {}ms, cmd={}, seq={}, channel={}",
                        t.getTimeout(TimeUnit.MILLISECONDS), f.command, f.seq, wd.getDtc().getChannel());
            }
            return ENCODE_CANCEL;
        }

        if (request) {
            f.seq = dtc.getAndIncSeq();
            f.timeout = rest;
        }
        encodeContext.reset();
        return doEncode(buf, wd);
    }

    private int doEncode(ByteBuffer buf, WriteData wd) {
        WritePacket wf = wd.getData();
        return wf.encode(encodeContext, buf) ? ENCODE_FINISH : ENCODE_NOT_FINISH;
    }

    public void setWriting(boolean writing) {
        this.writing = writing;
    }
}

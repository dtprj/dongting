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

import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.pb.PbUtil;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author huangli
 */
class DtChannel {
    private static final DtLog log = DtLogs.getLogger(DtChannel.class);

    private static final int INIT_BUF_SIZE = 2048;
    private static final int MAX_FRAME_SIZE = 8 * 1024 * 1024;

    private final NioStatus nioStatus;
    private final NioConfig nioConfig;
    private final WorkerParams workerParams;
    private final SocketChannel channel;

    private SelectionKey selectionKey;

    private WeakReference<ByteBuffer> readBufferCache;
    private ByteBuffer readBuffer;
    // read status
    private int currentReadFrameSize = -1;
    private int readBufferMark = 0;

    private int seq = 1;

    private final IoSubQueue subQueue;

    private boolean closed;

    public DtChannel(NioStatus nioStatus, WorkerParams workerParams, NioConfig nioConfig, SocketChannel socketChannel) {
        this.nioStatus = nioStatus;
        this.channel = socketChannel;
        this.subQueue = new IoSubQueue(this::registerForWrite, workerParams.getPool());
        this.nioConfig = nioConfig;
        this.workerParams = workerParams;
    }

    private void registerForWrite() {
        selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    public ByteBuffer getOrCreateReadBuffer() {
        if (readBuffer != null) {
            return readBuffer;
        }
        if (readBufferCache == null) {
            readBuffer = ByteBuffer.allocateDirect(INIT_BUF_SIZE);
            readBufferCache = new WeakReference<>(readBuffer);
            return readBuffer;
        } else {
            ByteBuffer cached = readBufferCache.get();
            if (cached != null) {
                return cached;
            } else {
                readBuffer = ByteBuffer.allocateDirect(INIT_BUF_SIZE);
                readBufferCache = new WeakReference<>(readBuffer);
                return readBuffer;
            }
        }
    }


    public void afterRead(boolean running) {
        ByteBuffer buf = this.readBuffer;
        // switch to read mode, but the start position may not 0, so we can't use flip()
        buf.limit(buf.position());
        buf.position(this.readBufferMark);

        for (int rest = buf.remaining(); rest >= this.currentReadFrameSize; rest = buf.remaining()) {
            if (currentReadFrameSize == -1) {
                // read frame length
                if (rest >= 4) {
                    currentReadFrameSize = buf.getInt();
                    if (currentReadFrameSize <= 0 || currentReadFrameSize > MAX_FRAME_SIZE) {
                        throw new DtException("frame too large: " + currentReadFrameSize);
                    }
                    readBufferMark = buf.position();
                } else {
                    break;
                    // wait next read
                }
            }
            if (currentReadFrameSize > 0) {
                readFrame(buf, running);
            }
        }

        prepareForNextRead();
    }

    private void readFrame(ByteBuffer buf, boolean running) {
        int currentReadFrameSize = this.currentReadFrameSize;
        if (buf.remaining() < currentReadFrameSize) {
            return;
        }
        int limit = buf.limit();
        buf.limit(buf.position() + currentReadFrameSize);
        ReadFrame f = new ReadFrame();
        RpcPbCallback pbCallback = this.workerParams.getCallback();
        pbCallback.setFrame(f);
        PbUtil.parse(buf, pbCallback);
        int pos = buf.position();

        int type = f.getFrameType();
        buf.position(pbCallback.getBodyStart());
        buf.limit(pbCallback.getBodyLimit());
        try {
            if (type == CmdType.TYPE_REQ) {
                processIncomingRequest(buf, f, running, currentReadFrameSize);
            } else if (type == CmdType.TYPE_RESP) {
                processIncomingResponse(buf, f);
            } else {
                log.warn("bad frame type: {}, {}", type, channel);
            }
        } finally {
            this.readBufferMark = pos;
            this.currentReadFrameSize = -1;
            buf.limit(limit);
            buf.position(pos);
        }
    }

    private void processIncomingResponse(ByteBuffer buf, ReadFrame resp) {
        WriteData wo = this.workerParams.getPendingRequests().remove(resp.getSeq());
        if (wo == null) {
            log.debug("pending request not found. channel={}, resp={}", channel, resp);
            return;
        }
        nioStatus.getRequestSemaphore().release();
        WriteFrame req = wo.getData();
        if (resp.getCommand() != req.getCommand()) {
            wo.getFuture().completeExceptionally(new NetException("command not match"));
            return;
        }
        Decoder decoder = wo.getDecoder();
        if (decoder.decodeInIoThread()) {
            try {
                Object body = decoder.decode(buf);
                resp.setBody(body);
            } catch (Throwable e) {
                if (log.isDebugEnabled()) {
                    log.debug("decode fail. {} {}", channel, e.toString());
                }
                wo.getFuture().completeExceptionally(new NetException(e));
            }
        } else {
            ByteBuffer bodyBuffer = ByteBuffer.allocate(buf.remaining());
            bodyBuffer.put(buf);
            bodyBuffer.flip();
            resp.setBody(bodyBuffer);
        }
        wo.getFuture().complete(resp);
    }

    private void processIncomingRequest(ByteBuffer buf, ReadFrame req, boolean running, int currentReadFrameSize) {
        if (!running) {
            writeErrorInIoThread(req, CmdCodes.STOPPING, null);
            return;
        }
        NioStatus nioStatus = this.nioStatus;
        ReqProcessor p = nioStatus.getProcessors().get(req.getCommand());
        if (p == null) {
            writeErrorInIoThread(req, CmdCodes.COMMAND_NOT_SUPPORT, null);
            return;
        }
        Decoder decoder = p.getDecoder();
        if (nioStatus.getBizExecutor() == null || p.runInIoThread()) {
            if (!fillBody(buf, req, decoder)) {
                return;
            }
            WriteFrame resp;
            try {
                resp = p.process(req, this);
            } catch (Throwable e) {
                writeErrorInIoThread(req, CmdCodes.BIZ_ERROR, e.toString());
                return;
            }
            if (resp != null) {
                resp.setCommand(req.getCommand());
                resp.setFrameType(CmdType.TYPE_RESP);
                resp.setSeq(req.getSeq());
                subQueue.enqueue(resp);
            } else {
                writeErrorInIoThread(req, CmdCodes.BIZ_ERROR, "processor return null response");
            }
        } else {
            if (decoder.decodeInIoThread()) {
                if (!fillBody(buf, req, decoder)) {
                    return;
                }
            } else {
                ByteBuffer body = ByteBuffer.allocate(buf.remaining());
                body.put(buf);
                body.flip();
                req.setBody(body);
            }
            AtomicLong bytes = nioStatus.getInReqBytes();
            // TODO can we eliminate this CAS operation?
            long bytesAfterAdd = bytes.addAndGet(currentReadFrameSize);
            if (bytesAfterAdd < nioConfig.getMaxInBytes()) {
                try {
                    // TODO use custom thread pool?
                    nioStatus.getBizExecutor().submit(() -> {
                        try {
                            processIncomingRequestInBizThreadPool(req, p, decoder);
                        } finally {
                            bytes.addAndGet(-currentReadFrameSize);
                        }
                    });
                } catch (RejectedExecutionException e) {
                    writeErrorInIoThread(req, CmdCodes.FLOW_CONTROL,
                            "max incoming request: " + nioConfig.getMaxInRequests());
                    bytes.addAndGet(-currentReadFrameSize);
                }
            } else {
                writeErrorInIoThread(req, CmdCodes.FLOW_CONTROL,
                        "max incoming request bytes: " + nioConfig.getMaxInBytes());
                bytes.addAndGet(-currentReadFrameSize);
            }
        }
    }

    private void processIncomingRequestInBizThreadPool(ReadFrame req, ReqProcessor p, Decoder decoder) {
        WriteFrame resp;
        try {
            if (!decoder.decodeInIoThread()) {
                ByteBuffer bodyBuffer = (ByteBuffer) req.getBody();
                req.setBody(decoder.decode(bodyBuffer));
            }
            resp = p.process(req, this);
        } catch (Throwable e) {
            writeErrorInBizThread(req, CmdCodes.BIZ_ERROR, e.toString());
            return;
        }
        if (resp != null) {
            resp.setCommand(req.getCommand());
            resp.setFrameType(CmdType.TYPE_RESP);
            resp.setSeq(req.getSeq());
            writeRespInBizThreads(resp);
        } else {
            writeErrorInBizThread(req, CmdCodes.BIZ_ERROR, "processor return null response");
        }
    }

    private boolean fillBody(ByteBuffer buf, ReadFrame req, Decoder decoder) {
        try {
            Object body = decoder.decode(buf);
            req.setBody(body);
            return true;
        } catch (Throwable e) {
            if (log.isDebugEnabled()) {
                log.debug("decode fail. {} {}", channel, e.toString());
            }
            writeErrorInIoThread(req, CmdCodes.BIZ_ERROR, e.toString());
            return false;
        }
    }

    private void writeErrorInIoThread(Frame req, int code, String msg) {
        ByteBufferWriteFrame resp = new ByteBufferWriteFrame();
        resp.setCommand(req.getCommand());
        resp.setFrameType(CmdType.TYPE_RESP);
        resp.setSeq(req.getSeq());
        resp.setRespCode(code);
        resp.setMsg(msg);
        subQueue.enqueue(resp);
    }

    private void writeErrorInBizThread(Frame req, int code, String msg) {
        ByteBufferWriteFrame resp = new ByteBufferWriteFrame();
        resp.setCommand(req.getCommand());
        resp.setFrameType(CmdType.TYPE_RESP);
        resp.setSeq(req.getSeq());
        resp.setRespCode(code);
        resp.setMsg(msg);
        writeRespInBizThreads(resp);
    }

    private void prepareForNextRead() {
        ByteBuffer buf = readBuffer;
        int endIndex = buf.limit();
        int capacity = buf.capacity();

        int frameSize = this.currentReadFrameSize;
        int mark = this.readBufferMark;
        if (frameSize == -1) {
            if (mark == capacity) {
                // read complete and buffer exhausted
                buf.clear();
                this.readBufferMark = 0;
            } else {
                int c = endIndex - mark;
                assert c >= 0 && c < 4;
                if (capacity - endIndex < 4 - c) {
                    // move length bytes to start
                    for (int i = 0; i < c; i++) {
                        buf.put(i, buf.get(mark + i));
                    }
                    buf.position(c);
                    buf.limit(capacity);
                    this.readBufferMark = 0;
                } else {
                    // continue read
                    buf.position(endIndex);
                    buf.limit(capacity);
                }
            }
        } else {
            int c = endIndex - mark;
            if (capacity - endIndex >= frameSize - c) {
                // there is enough space for this frame, return, continue read
                buf.position(endIndex);
                buf.limit(capacity);
            } else {
                buf.limit(endIndex);
                buf.position(mark);
                // TODO array copy
                if (frameSize > capacity) {
                    // allocate bigger buffer
                    ByteBuffer newBuffer = ByteBuffer.allocateDirect(frameSize);
                    newBuffer.put(buf);
                    this.readBuffer = newBuffer;
                    this.readBufferCache = new WeakReference<>(newBuffer);
                } else {
                    // copy bytes to start
                    ByteBuffer newBuffer = buf.slice();
                    buf.clear();
                    buf.put(newBuffer);
                }
                this.readBufferMark = 0;
            }
        }
    }

    // invoke by other threads
    private void writeRespInBizThreads(WriteFrame frame) {
        WriteData data = new WriteData(this, frame);
        WorkerParams wp = this.workerParams;
        wp.getIoQueue().write(data);
        wp.getWakeupRunnable().run();
    }

    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public int getAndIncSeq() {
        return seq++;
    }

    public IoSubQueue getSubQueue() {
        return subQueue;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        this.closed = true;
    }

    public String getWorkerName() {
        return this.workerParams.getWorkerName();
    }

    @Override
    public String toString() {
        return "DTC[" + channel + "]";
    }
}

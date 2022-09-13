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

import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.pb.PbUtil;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
class DtChannel {
    private static final DtLog log = DtLogs.getLogger(DtChannel.class);

    private static final int INIT_BUF_SIZE = 2048;
    private static final int MAX_FRAME_SIZE = 8 * 1024 * 1024;
    private final RpcPbCallback pbCallback;
    private final NioStatus nioStatus;
    private final SocketChannel channel;
    private final Runnable wakeupRunnable;
    private final IoQueue ioQueue;
    private SelectionKey selectionKey;

    private WeakReference<ByteBuffer> readBufferCache;
    private ByteBuffer readBuffer;
    // read status
    private int currentReadFrameSize = -1;
    private int readBufferMark = 0;

    private final HashMap<Integer, WriteData> pendingRequests;
    private int seq = 1;

    private final IoSubQueue subQueue;

    public DtChannel(NioStatus nioStatus, WorkerParams workerParams) {
        this.nioStatus = nioStatus;
        this.pbCallback = workerParams.getCallback();
        this.channel = workerParams.getChannel();
        this.ioQueue = workerParams.getIoQueue();
        this.wakeupRunnable = workerParams.getWakeupRunnable();
        this.pendingRequests = workerParams.getPendingRequests();
        this.subQueue = new IoSubQueue(this::registerForWrite, workerParams.getPool());
    }

    public SocketChannel getChannel() {
        return channel;
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
        if (buf.remaining() < this.currentReadFrameSize) {
            return;
        }
        int limit = buf.limit();
        buf.limit(buf.position() + this.currentReadFrameSize);
        ReadFrame f = new ReadFrame();
        RpcPbCallback pbCallback = this.pbCallback;
        pbCallback.setFrame(f);
        PbUtil.parse(buf, pbCallback);
        buf.limit(limit);
        this.readBufferMark = buf.position();
        this.currentReadFrameSize = -1;
        int type = f.getFrameType();
        if (type == CmdType.TYPE_REQ) {
            processIncomingRequest(pbCallback, buf, f, running);
        } else if (type == CmdType.TYPE_RESP) {
            processIncomingResponse(pbCallback, buf, f);
        } else {
            log.warn("bad frame type: {}, {}", type, channel);
        }
    }

    private void processIncomingResponse(RpcPbCallback pbCallback, ByteBuffer buf, ReadFrame resp) {
        WriteData wo = pendingRequests.remove(resp.getSeq());
        if (wo == null) {
            log.debug("pending request not found. channel={}, resp={}", channel, resp);
            return;
        }
        nioStatus.getRequestSemaphore().release();
        WriteFrame req = wo.getData();
        if (resp.getCommand() != req.getCommand()) {
            wo.getFuture().completeExceptionally(new RemotingException("command not match"));
            return;
        }
        Decoder decoder = wo.getDecoder();
        int pos = buf.position();
        int limit = buf.limit();
        buf.position(pbCallback.getBodyStart());
        buf.limit(pbCallback.getBodyLimit());
        if (decoder.decodeInIoThread()) {
            try {
                Object body = decoder.decode(buf);
                resp.setBody(body);
            } catch (Throwable e) {
                if (log.isDebugEnabled()) {
                    log.debug("decode fail. {} {}", channel, e.toString());
                }
                wo.getFuture().completeExceptionally(new RemotingException(e));
            }
        } else {
            ByteBuffer bodyBuffer = ByteBuffer.allocate(limit - pos);
            bodyBuffer.put(buf);
            bodyBuffer.flip();
            resp.setBody(bodyBuffer);
        }
        buf.limit(limit);
        buf.position(pos);
        wo.getFuture().complete(resp);
    }

    private void processIncomingRequest(RpcPbCallback pbCallback, ByteBuffer buf, ReadFrame req, boolean running) {
        if (!running) {
            writeErrorInIoThread(req, CmdCodes.STOPPING, null);
            return;
        }
        ReqProcessor p = nioStatus.getProcessor(req.getCommand());
        if (p == null) {
            writeErrorInIoThread(req, CmdCodes.COMMAND_NOT_SUPPORT, null);
            return;
        }
        Decoder decoder = p.getDecoder();
        if (nioStatus.getBizExecutor() == null || p.runInIoThread()) {
            if (!fillBody(pbCallback, buf, req, decoder)) {
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
                if(!fillBody(pbCallback, buf, req, decoder)){
                    return;
                }
            } else {
                int pos = buf.position();
                int limit = buf.limit();
                buf.position(pbCallback.getBodyStart());
                buf.limit(pbCallback.getBodyLimit());

                ByteBuffer body = ByteBuffer.allocate(buf.remaining());
                body.put(buf);
                body.flip();

                buf.limit(limit);
                buf.position(pos);
                req.setBody(body);
            }
            nioStatus.getBizExecutor().submit(() -> processIncomingRequestInBizThreadPool(req, p, decoder));
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

    private boolean fillBody(RpcPbCallback pbCallback, ByteBuffer buf, ReadFrame req, Decoder decoder) {
        int pos = buf.position();
        int limit = buf.limit();
        buf.position(pbCallback.getBodyStart());
        buf.limit(pbCallback.getBodyLimit());
        try {
            Object body = decoder.decode(buf);
            req.setBody(body);
            buf.limit(limit);
            buf.position(pos);
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
        WriteData data = new WriteData(this, frame, null, null, null);
        this.ioQueue.write(data);
        this.wakeupRunnable.run();
    }

    // invoke by other threads
    public void writeReqInBizThreads(WriteFrame frame, Decoder decoder, DtTime timeout, CompletableFuture<ReadFrame> future) {
        Objects.requireNonNull(timeout);
        Objects.requireNonNull(future);

        WriteData data = new WriteData(this, frame, timeout, future, decoder);
        this.ioQueue.write(data);
        this.wakeupRunnable.run();
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
}

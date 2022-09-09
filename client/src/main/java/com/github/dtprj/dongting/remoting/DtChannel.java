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

    private final HashMap<Integer, WriteRequest> pendingRequests;
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
        pbCallback.setFrame(f);
        PbUtil.parse(buf, pbCallback);
        buf.limit(limit);
        this.readBufferMark = buf.position();
        this.currentReadFrameSize = -1;
        int type = f.getFrameType();
        if (type == CmdType.TYPE_REQ) {
            processIncomingRequest(f, running);
        } else if (type == CmdType.TYPE_RESP) {
            processIncomingResponse(f);
        } else {
            log.warn("bad frame type: {}, {}", type, channel);
        }
    }

    private void processIncomingResponse(ReadFrame resp) {
        WriteRequest wo = pendingRequests.remove(resp.getSeq());
        if (wo == null) {
            log.debug("pending request not found. channel={}, resp={}", channel, resp);
            return;
        }
        nioStatus.getRequestSemaphore().release();
        Frame req = wo.getData();
        if (resp.getCommand() != req.getCommand()) {
            wo.getFuture().completeExceptionally(new RemotingException("command not match"));
            return;
        }
        wo.getFuture().complete(resp);
    }

    private void processIncomingRequest(ReadFrame req, boolean running) {
        if (!running) {
            writeErrorInWorkerThread(req, CmdCodes.STOPPING);
            return;
        }
        CmdProcessor p = nioStatus.getProcessor(req.getCommand());
        if (p == null) {
            writeErrorInWorkerThread(req, CmdCodes.COMMAND_NOT_SUPPORT);
        } else {
            if (nioStatus.getBizExecutor() == null) {
                WriteFrame resp = p.process(req, this);
                resp.setCommand(req.getCommand());
                resp.setFrameType(CmdType.TYPE_RESP);
                resp.setSeq(req.getSeq());
                subQueue.enqueue(resp);
            } else {
                nioStatus.getBizExecutor().submit(() -> {
                    WriteFrame resp = p.process(req, this);
                    resp.setCommand(req.getCommand());
                    resp.setFrameType(CmdType.TYPE_RESP);
                    resp.setSeq(req.getSeq());
                    writeResp(resp);
                });
            }
        }
    }

    private void writeErrorInWorkerThread(Frame req, int code) {
        WriteFrame resp = new WriteFrame();
        resp.setCommand(req.getCommand());
        resp.setFrameType(CmdType.TYPE_RESP);
        resp.setSeq(req.getSeq());
        resp.setRespCode(code);
        subQueue.enqueue(resp);
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
    private void writeResp(WriteFrame frame) {
        WriteRequest data = new WriteRequest(this, frame, null, null);
        this.ioQueue.write(data);
        this.wakeupRunnable.run();
    }

    // invoke by other threads
    public void writeReq(WriteFrame frame, DtTime timeout, CompletableFuture<ReadFrame> future) {
        Objects.requireNonNull(timeout);
        Objects.requireNonNull(future);

        WriteRequest data = new WriteRequest(this, frame, timeout, future);
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

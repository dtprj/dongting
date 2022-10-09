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

import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.pb.PbException;
import com.github.dtprj.dongting.pb.PbParser;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author huangli
 */
class DtChannel implements PbCallback {
    private static final DtLog log = DtLogs.getLogger(DtChannel.class);

    private final NioStatus nioStatus;
    private final NioConfig nioConfig;
    private final WorkerParams workerParams;
    private final SocketChannel channel;
    private Peer peer;

    private final int channelIndexInWorker;
    int seq = 1;

    // read status
    private final ArrayList<ReadFrameInfo> frames = new ArrayList<>();
    private ReadFrame frame;
    private ReadFrameInfo readFrameInfo;
    private WriteData writeDataForResp;
    private ReqProcessor processorForRequest;
    private int currentReadFrameSize;
    private Object decodeStatus;
    private final PbParser parser;
    private boolean drop;

    private boolean running;

    private final IoSubQueue subQueue;

    private boolean closed;

    private static class ReadFrameInfo {
        ReadFrame frame;
        WriteData writeDataForResp;
        ReqProcessor processorForRequest;
    }

    public DtChannel(NioStatus nioStatus, WorkerParams workerParams, NioConfig nioConfig,
                     SocketChannel socketChannel, int channelIndexInWorker) {
        this.nioStatus = nioStatus;
        this.channel = socketChannel;
        this.subQueue = new IoSubQueue(workerParams.getPool());
        this.nioConfig = nioConfig;
        this.workerParams = workerParams;
        this.channelIndexInWorker = channelIndexInWorker;
        this.parser = new PbParser(nioConfig.getMaxFrameSize());
    }

    public void afterRead(boolean running, ByteBuffer buf) {
        if (!running) {
            this.running = false;
        }
        parser.parse(buf, this);
        for (ReadFrameInfo rfi : frames) {
            ReadFrame f = rfi.frame;
            if (f.getFrameType() == FrameType.TYPE_RESP) {
                processIncomingResponse(f, rfi.writeDataForResp);
            } else {
                processIncomingRequest(f, rfi.processorForRequest);
            }
        }
        frames.clear();
    }

    @Override
    public void begin(int len) {
        this.currentReadFrameSize = len;
        frame = new ReadFrame();
        readFrameInfo = new ReadFrameInfo();
        readFrameInfo.frame = frame;
        drop = false;
        writeDataForResp = null;
        processorForRequest = null;
        decodeStatus = null;
    }

    @Override
    public void end() {
        readFrameInfo.writeDataForResp = writeDataForResp;
        readFrameInfo.processorForRequest = processorForRequest;
        frames.add(readFrameInfo);
    }

    @Override
    public void readVarInt(int index, long value) {
        switch (index) {
            case Frame.IDX_TYPE:
                frame.setFrameType((int) value);
                break;
            case Frame.IDX_COMMAND:
                frame.setCommand((int) value);
                break;
            case Frame.IDX_SEQ:
                frame.setSeq((int) value);
                break;
            case Frame.IDX_RESP_CODE:
                frame.setRespCode((int) value);
                break;
            default:
                throw new PbException("readInt " + index);
        }
    }

    @Override
    public void readBytes(int index, ByteBuffer buf, int len, boolean start, boolean end) {
        switch (index) {
            case Frame.IDX_MSG: {
                // TODO finish it
                break;
            }
            case Frame.IDX_BODY: {
                readBody(buf, len, start, end);
                break;
            }
            default:
                throw new PbException("readInt " + index);
        }
    }

    private void readBody(ByteBuffer buf, int len, boolean start, boolean end) {
        if (frame.getCommand() <= 0) {
            throw new NetException("command invalid :" + frame.getCommand());
        }
        int type = frame.getFrameType();
        if (drop) {
            // TODO abort parse
            return;
        }
        // the body field should encode as last field
        Decoder decoder;
        if (type == FrameType.TYPE_RESP) {
            if (writeDataForResp == null) {
                writeDataForResp = this.workerParams.getPendingRequests().remove(BitUtil.toLong(channelIndexInWorker, frame.getSeq()));
            }
            if (writeDataForResp == null) {
                drop = true;
                log.debug("pending request not found. channel={}, resp={}", channel, frame);
                return;
            }
            decoder = writeDataForResp.getDecoder();
        } else {
            if (!running) {
                drop = true;
                writeErrorInIoThread(frame, CmdCodes.STOPPING, null);
                return;
            }
            if (processorForRequest == null) {
                processorForRequest = nioStatus.getProcessors().get(frame.getCommand());
            }
            if (processorForRequest == null) {
                drop = true;
                writeErrorInIoThread(frame, CmdCodes.COMMAND_NOT_SUPPORT, null);
                return;
            }
            decoder = processorForRequest.getDecoder();
        }

        boolean copy;
        int decode;//0 not decode, 1 use io buffer decode, 2 use copy buffer decode
        if (!decoder.decodeInIoThread() && nioStatus.getBizExecutor() != null) {
            copy = true;
            decode = 0;
        } else {
            if (decoder.supportHalfPacket()) {
                copy = false;
                decode = 1;
            } else {
                if (start && end) {
                    // full frame
                    copy = false;
                    decode = 1;
                } else {
                    copy = true;
                    decode = 2;
                }
            }
        }
        if (copy) {
            copyBuffer(buf, len, start, end);
        }
        try {
            if (decode == 1) {
                decodeStatus = decoder.decode(decodeStatus, buf, currentReadFrameSize, start, end);
                if (end) {
                    frame.setBody(decodeStatus);
                }
            } else if (decode == 2) {
                Object result = decoder.decode(null, (ByteBuffer) frame.getBody(),
                        currentReadFrameSize, true, true);
                frame.setBody(result);
            }
        } catch (Throwable e) {
            processIoDecodeFail(e);
        }

        if (end) {
            // so if the body is not last field, exception throws
            this.frame = null;
        }
    }

    private void copyBuffer(ByteBuffer buf, int len, boolean start, boolean end) {
        ByteBuffer body;
        if (start) {
            body = ByteBuffer.allocate(len);
            frame.setBody(body);
        } else {
            body = (ByteBuffer) frame.getBody();
        }
        body.put(buf);
        if (end) {
            body.flip();
        }
    }

    private void processIoDecodeFail(Throwable e) {
        if (log.isDebugEnabled()) {
            log.debug("decode fail. {} {}", channel, e.toString());
        }
        if (writeDataForResp != null) {
            writeDataForResp.getFuture().completeExceptionally(e);
        }
        drop = true;
    }

    private void processIncomingResponse(ReadFrame resp, WriteData wo) {
        nioStatus.getRequestSemaphore().release();
        WriteFrame req = wo.getData();
        if (resp.getCommand() != req.getCommand()) {
            wo.getFuture().completeExceptionally(new NetException("command not match"));
            return;
        }
        wo.getFuture().complete(resp);
    }

    private void processIncomingRequest(ReadFrame req, ReqProcessor p) {
        if (nioStatus.getBizExecutor() == null || p.runInIoThread()) {
            WriteFrame resp;
            try {
                resp = p.process(req, this);
            } catch (Throwable e) {
                writeErrorInIoThread(req, CmdCodes.BIZ_ERROR, e.toString());
                return;
            }
            if (resp != null) {
                resp.setCommand(req.getCommand());
                resp.setFrameType(FrameType.TYPE_RESP);
                resp.setSeq(req.getSeq());
                subQueue.enqueue(resp);
            } else {
                writeErrorInIoThread(req, CmdCodes.BIZ_ERROR, "processor return null response");
            }
        } else {
            AtomicLong bytes = nioStatus.getInReqBytes();
            // TODO can we eliminate this CAS operation?
            long bytesAfterAdd = bytes.addAndGet(currentReadFrameSize);
            if (bytesAfterAdd < nioConfig.getMaxInBytes()) {
                try {
                    // TODO use custom thread pool?
                    nioStatus.getBizExecutor().submit(new ProcessInBizThreadTask(
                            req, p, currentReadFrameSize, this, bytes));
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

    private static class ProcessInBizThreadTask implements Runnable {
        private final ReadFrame req;
        private final ReqProcessor processor;
        private final int frameSize;
        private final DtChannel dtc;
        private final AtomicLong inBytes;

        ProcessInBizThreadTask(ReadFrame req, ReqProcessor processor, int frameSize, DtChannel dtc, AtomicLong inBytes) {
            this.req = req;
            this.processor = processor;
            this.frameSize = frameSize;
            this.dtc = dtc;
            this.inBytes = inBytes;
        }

        @Override
        public void run() {
            try {
                WriteFrame resp;
                Decoder decoder = processor.getDecoder();
                try {
                    if (!decoder.decodeInIoThread()) {
                        ByteBuffer bodyBuffer = (ByteBuffer) req.getBody();
                        req.setBody(decoder.decode(null, bodyBuffer, frameSize, true, true));
                    }
                    resp = processor.process(req, dtc);
                } catch (Throwable e) {
                    dtc.writeErrorInBizThread(req, CmdCodes.BIZ_ERROR, e.toString());
                    return;
                }
                if (resp != null) {
                    resp.setCommand(req.getCommand());
                    resp.setFrameType(FrameType.TYPE_RESP);
                    resp.setSeq(req.getSeq());
                    dtc.writeRespInBizThreads(resp);
                } else {
                    dtc.writeErrorInBizThread(req, CmdCodes.BIZ_ERROR, "processor return null response");
                }
            } finally {
                inBytes.addAndGet(-frameSize);
            }
        }
    }

    private void writeErrorInIoThread(Frame req, int code, String msg) {
        ByteBufferWriteFrame resp = new ByteBufferWriteFrame();
        resp.setCommand(req.getCommand());
        resp.setFrameType(FrameType.TYPE_RESP);
        resp.setSeq(req.getSeq());
        resp.setRespCode(code);
        resp.setMsg(msg);
        subQueue.enqueue(resp);
    }

    private void writeErrorInBizThread(Frame req, int code, String msg) {
        ByteBufferWriteFrame resp = new ByteBufferWriteFrame();
        resp.setCommand(req.getCommand());
        resp.setFrameType(FrameType.TYPE_RESP);
        resp.setSeq(req.getSeq());
        resp.setRespCode(code);
        resp.setMsg(msg);
        writeRespInBizThreads(resp);
    }

    // invoke by other threads
    private void writeRespInBizThreads(WriteFrame frame) {
        WriteData data = new WriteData(this, frame);
        WorkerParams wp = this.workerParams;
        wp.getIoQueue().write(data);
        wp.getWakeupRunnable().run();
    }

    public int getAndIncSeq() {
        return seq++;
    }

    public int getChannelIndexInWorker() {
        return channelIndexInWorker;
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
        if (closed) {
            return;
        }
        this.closed = true;
        if (peer != null && peer.getDtChannel() == this) {
            peer.setDtChannel(null);
        }
    }

    public void setPeer(Peer peer) {
        this.peer = peer;
    }

    @Override
    public String toString() {
        return "DTC[" + channel + "]";
    }
}

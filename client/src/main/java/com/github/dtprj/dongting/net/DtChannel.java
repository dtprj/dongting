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
import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.pb.PbException;
import com.github.dtprj.dongting.pb.PbParser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author huangli
 */
class DtChannel extends PbCallback {
    private static final DtLog log = DtLogs.getLogger(DtChannel.class);

    private final NioStatus nioStatus;
    private final NioConfig nioConfig;
    private final WorkerStatus workerStatus;
    private final SocketChannel channel;
    private final ProcessContext processContext;
    private Peer peer;

    private final int channelIndexInWorker;
    int seq = 1;

    // read status
    private final ArrayList<ReadFrameInfo> frames = new ArrayList<>();
    private final PbParser parser;
    private final StringFieldDecoder strDecoder;
    private ReadFrame frame;
    private boolean readBody;
    private WriteData writeDataForResp;
    private ReqProcessor processorForRequest;
    private int currentReadFrameSize;

    private boolean running = true;

    private final IoSubQueue subQueue;

    private boolean closed;

    private static class ReadFrameInfo {
        ReadFrame frame;
        WriteData writeDataForResp;
        ReqProcessor processorForRequest;
    }

    public DtChannel(NioStatus nioStatus, WorkerStatus workerStatus, NioConfig nioConfig,
                     SocketChannel socketChannel, int channelIndexInWorker) throws IOException {
        this.nioStatus = nioStatus;
        this.channel = socketChannel;
        this.subQueue = new IoSubQueue(workerStatus.getDirectPool(), workerStatus.getHeapPool());
        this.nioConfig = nioConfig;
        this.workerStatus = workerStatus;
        this.channelIndexInWorker = channelIndexInWorker;
        this.parser = PbParser.multiParser(this, nioConfig.getMaxFrameSize());
        this.strDecoder = new StringFieldDecoder(workerStatus.getHeapPool());

        ProcessContext processContext = new ProcessContext();
        processContext.setChannel(socketChannel);
        processContext.setRemoteAddr(socketChannel.getRemoteAddress());
        processContext.setLocalAddr(socketChannel.getLocalAddress());
        processContext.setIoThreadStrDecoder(strDecoder);
        processContext.setIoHeapBufferPool(new ByteBufferPoolReleaseInOtherThread(
                workerStatus.getHeapPool(), workerStatus.getIoQueue()));
        this.processContext = processContext;
    }

    public void afterRead(boolean running, ByteBuffer buf) {
        if (!running) {
            this.running = false;
        }
        parser.parse(buf);
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
        readBody = false;
        writeDataForResp = null;
        processorForRequest = null;
    }

    @Override
    public void end(boolean success) {
        if (!success) {
            return;
        }
        WriteData writeDataForResp = this.writeDataForResp;
        ReqProcessor processorForRequest = this.processorForRequest;
        if (writeDataForResp == null && processorForRequest == null) {
            // empty body
            initRelatedDataForFrame(false);
            writeDataForResp = this.writeDataForResp;
            processorForRequest = this.processorForRequest;
            if (writeDataForResp == null && processorForRequest == null) {
                return;
            }
        }
        ReadFrameInfo readFrameInfo = new ReadFrameInfo();
        readFrameInfo.frame = frame;
        readFrameInfo.writeDataForResp = writeDataForResp;
        readFrameInfo.processorForRequest = processorForRequest;
        frames.add(readFrameInfo);
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        if (readBody) {
            throw new PbException("body has read");
        }
        switch (index) {
            case Frame.IDX_TYPE:
                frame.setFrameType((int) value);
                break;
            case Frame.IDX_COMMAND:
                frame.setCommand((int) value);
                break;
            case Frame.IDX_RESP_CODE:
                frame.setRespCode((int) value);
                break;
            default:
                throw new PbException("unexpect index in readVarInt, index=" + index);
        }
        return true;
    }

    @Override
    public boolean readFix32(int index, int value) {
        if (index != Frame.IDX_SEQ) {
            throw new PbException("bad seq index: " + index);
        }
        frame.setSeq(value);
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        return false;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, boolean start, boolean end) {
        if (this.readBody) {
            throw new PbException("body has read");
        }
        switch (index) {
            case Frame.IDX_MSG: {
                String msg = strDecoder.decodeUTF8(buf, fieldLen, start, end);
                if (msg != null) {
                    this.frame.setMsg(msg);
                }
                return true;
            }
            case Frame.IDX_BODY: {
                return readBody(buf, fieldLen, start, end);
            }
            default:
                throw new PbException("unexpect index in readBytes, index=" + index);
        }
    }

    private boolean readBody(ByteBuffer buf, int fieldLen, boolean start, boolean end) {
        ReadFrame frame = this.frame;
        if (frame.getCommand() <= 0) {
            throw new NetException("command invalid :" + frame.getCommand());
        }
        // the body field should encode as last field
        Decoder decoder = initRelatedDataForFrame(true);
        if (decoder == null) {
            return false;
        }

        boolean decodeInIoThread = decoder.decodeInIoThread();
        boolean useIoBufferDecode = decodeInIoThread && decoder.supportHalfPacket();
        try {
            if (useIoBufferDecode) {
                Object o = decoder.decode(processContext, buf, fieldLen, start, end);
                if (end) {
                    frame.setBody(o);
                }
            } else {
                if (decodeInIoThread) {
                    if (start && end) {
                        Object o = decoder.decode(processContext, buf, fieldLen, true, true);
                        frame.setBody(o);
                    } else {
                        ByteBuffer copyBuffer = copyBuffer(buf, fieldLen, start, end);
                        if (end) {
                            try {
                                Object result = decoder.decode(processContext, copyBuffer,
                                        fieldLen, false, true);
                                frame.setBody(result);
                            } finally {
                                workerStatus.getHeapPool().release(copyBuffer);
                            }
                        }
                    }
                } else {
                    copyBuffer(buf, fieldLen, start, end);
                }
            }
        } catch (Throwable e) {
            processIoDecodeFail(e);
            return false;
        }

        if (end) {
            processContext.setIoDecodeStatus(null);
            // so if the body is not last field, exception throws
            readBody = true;
        }
        return true;
    }

    private Decoder initRelatedDataForFrame(boolean returnDecoder) {
        ReadFrame frame = this.frame;
        if (frame.getFrameType() == FrameType.TYPE_RESP) {
            WriteData writeDataForResp = this.writeDataForResp;
            if (writeDataForResp == null) {
                writeDataForResp = this.workerStatus.getPendingRequests().remove(BitUtil.toLong(channelIndexInWorker, frame.getSeq()));
                if (writeDataForResp == null) {
                    log.info("pending request not found. channel={}, resp={}", channel, frame);
                    return null;
                } else {
                    this.writeDataForResp = writeDataForResp;
                }
            }
            if (returnDecoder) {
                return writeDataForResp.getDecoder();
            } else {
                return null;
            }
        } else {
            if (!running) {
                log.debug("the channel is closing...");
                writeErrorInIoThread(frame, CmdCodes.STOPPING, null);
                return null;
            }
            ReqProcessor processorForRequest = this.processorForRequest;
            if (processorForRequest == null) {
                processorForRequest = nioStatus.getProcessor(frame.getCommand());
                if (processorForRequest == null) {
                    log.warn("command {} is not support", frame.getCommand());
                    writeErrorInIoThread(frame, CmdCodes.COMMAND_NOT_SUPPORT, null);
                    return null;
                }
                if (processorForRequest.getExecutor() != null) {
                    // TODO can we eliminate this CAS operation?
                    long bytesAfterAdd = nioStatus.getInReqBytes().addAndGet(this.currentReadFrameSize);
                    if (bytesAfterAdd > nioConfig.getMaxInBytes()) {
                        log.debug("pendingBytes({})>maxInBytes({}), write response code FLOW_CONTROL to client",
                                bytesAfterAdd, nioConfig.getMaxInBytes());
                        writeErrorInIoThread(frame, CmdCodes.FLOW_CONTROL,
                                "max incoming request bytes: " + nioConfig.getMaxInBytes());
                        nioStatus.getInReqBytes().addAndGet(-currentReadFrameSize);
                        return null;
                    }
                }
                this.processorForRequest = processorForRequest;
            }
            if (returnDecoder) {
                return processorForRequest.getDecoder();
            } else {
                return null;
            }
        }
    }

    private ByteBuffer copyBuffer(ByteBuffer buf, int fieldLen, boolean start, boolean end) {
        ByteBuffer body;
        ReadFrame frame = this.frame;
        if (start) {
            body = workerStatus.getHeapPool().borrow(fieldLen);
            frame.setBody(body);
        } else {
            body = (ByteBuffer) frame.getBody();
        }
        body.put(buf);
        if (end) {
            body.flip();
        }
        return body;
    }

    private void processIoDecodeFail(Throwable e) {
        if (log.isDebugEnabled()) {
            log.debug("decode fail. {} {}", channel, e.toString());
        }
        if (frame.getFrameType() == FrameType.TYPE_RESP) {
            if (writeDataForResp != null) {
                writeDataForResp.getFuture().completeExceptionally(e);
            }
        } else {
            log.warn("decode fail in io thread", e);
            writeErrorInIoThread(frame, CmdCodes.BIZ_ERROR, "decode fail: " + e.toString());
        }
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
        //TODO drop timeout requests
        NioStatus nioStatus = this.nioStatus;
        if (p.getExecutor() == null) {
            WriteFrame resp;
            try {
                resp = p.process(req, processContext);
            } catch (Throwable e) {
                log.warn("ReqProcessor.process fail", e);
                writeErrorInIoThread(req, CmdCodes.BIZ_ERROR, e.toString());
                return;
            }
            if (resp != null) {
                resp.setCommand(req.getCommand());
                resp.setFrameType(FrameType.TYPE_RESP);
                resp.setSeq(req.getSeq());
                subQueue.enqueue(resp);
            } else {
                log.warn("ReqProcessor.process return null");
                writeErrorInIoThread(req, CmdCodes.BIZ_ERROR, "processor return null response");
            }
        } else {
            AtomicLong bytes = nioStatus.getInReqBytes();
            int currentReadFrameSize = this.currentReadFrameSize;
            ByteBuffer bufferNeedRelease = null; // copy buffer need to release
            if (!p.getDecoder().decodeInIoThread()) {
                bufferNeedRelease = (ByteBuffer) req.getBody();
            }
            try {
                // TODO use custom thread pool?
                p.getExecutor().submit(new ProcessInBizThreadTask(
                        req, p, currentReadFrameSize, this, bytes));
            } catch (RejectedExecutionException e) {
                log.debug("catch RejectedExecutionException, write response code FLOW_CONTROL to client, maxInRequests={}",
                        nioConfig.getMaxInRequests());
                writeErrorInIoThread(req, CmdCodes.FLOW_CONTROL,
                        "max incoming request: " + nioConfig.getMaxInRequests());
                bytes.addAndGet(-currentReadFrameSize);
                if (bufferNeedRelease != null) {
                    workerStatus.getHeapPool().release(bufferNeedRelease);
                }
            }
        }
    }

    private void writeErrorInIoThread(Frame req, int code, String msg) {
        ByteBufferWriteFrame resp = new ByteBufferWriteFrame(null);
        resp.setCommand(req.getCommand());
        resp.setFrameType(FrameType.TYPE_RESP);
        resp.setSeq(req.getSeq());
        resp.setRespCode(code);
        resp.setMsg(msg);
        subQueue.enqueue(resp);
    }

    public void writeBizErrorInBizThread(Frame req, String msg) {
        ByteBufferWriteFrame resp = new ByteBufferWriteFrame(null);
        resp.setCommand(req.getCommand());
        resp.setFrameType(FrameType.TYPE_RESP);
        resp.setSeq(req.getSeq());
        resp.setRespCode(CmdCodes.BIZ_ERROR);
        resp.setMsg(msg);
        writeRespInBizThreads(resp);
    }

    // invoke by other threads
    public void writeRespInBizThreads(WriteFrame frame) {
        WriteData data = new WriteData(this, frame);
        WorkerStatus wp = this.workerStatus;
        wp.getIoQueue().writeFromBizThread(data);
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

    public WorkerStatus getWorkerStatus() {
        return workerStatus;
    }

    public ProcessContext getProcessContext() {
        return processContext;
    }

    public void close() {
        if (closed) {
            return;
        }
        this.closed = true;
    }

    public void setPeer(Peer peer) {
        this.peer = peer;
    }

    public Peer getPeer() {
        return peer;
    }

    // for unit test
    PbParser getParser() {
        return parser;
    }

    // for unit test
    ReadFrame getFrame() {
        return frame;
    }
}

class ProcessInBizThreadTask implements Runnable {
    private static final DtLog log = DtLogs.getLogger(ProcessInBizThreadTask.class);
    private final ReadFrame req;
    private final ReqProcessor processor;
    private final int frameSize;
    private final DtChannel dtc;
    private final AtomicLong inBytes;

    ProcessInBizThreadTask(ReadFrame req, ReqProcessor processor,
                           int frameSize, DtChannel dtc, AtomicLong inBytes) {
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
            ReadFrame req = this.req;
            DtChannel dtc = this.dtc;
            boolean decodeSuccess = false;
            try {
                ReqProcessor processor = this.processor;
                Decoder decoder = processor.getDecoder();
                if (!processor.getDecoder().decodeInIoThread()) {
                    ByteBuffer bodyBuffer = (ByteBuffer) req.getBody();
                    if (bodyBuffer != null) {
                        try {
                            Object o = decoder.decode(null, bodyBuffer, bodyBuffer.remaining(), true, true);
                            req.setBody(o);
                        } finally {
                            WorkerStatus ws = dtc.getWorkerStatus();
                            ReleaseBufferTask t = new ReleaseBufferTask(ws.getHeapPool(), bodyBuffer);
                            ws.getIoQueue().scheduleFromBizThread(t);
                        }
                    }
                }
                decodeSuccess = true;
                resp = processor.process(req, dtc.getProcessContext());
            } catch (Throwable e) {
                if (decodeSuccess) {
                    log.warn("ReqProcessor.process fail, command={}", req.getCommand(), e);
                } else {
                    log.warn("ReqProcessor decode fail, command={}", req.getCommand(), e);
                }
                dtc.writeBizErrorInBizThread(req, e.toString());
                return;
            }
            if (resp != null) {
                resp.setCommand(req.getCommand());
                resp.setFrameType(FrameType.TYPE_RESP);
                resp.setSeq(req.getSeq());
                dtc.writeRespInBizThreads(resp);
            } else {
                log.warn("ReqProcessor.process return null, command={}", req.getCommand());
                dtc.writeBizErrorInBizThread(req, "processor return null response");
            }
        } finally {
            inBytes.addAndGet(-frameSize);
        }
    }
}

class ByteBufferPoolReleaseInOtherThread extends ByteBufferPool {
    private final SimpleByteBufferPool pool;
    private final IoQueue ioQueue;

    ByteBufferPoolReleaseInOtherThread(SimpleByteBufferPool pool, IoQueue ioQueue) {
        this.pool = pool;
        this.ioQueue = ioQueue;
    }

    @Override
    public ByteBuffer borrow(int requestSize) {
        return pool.borrow(requestSize);
    }

    @Override
    public void release(ByteBuffer buf) {
        ioQueue.scheduleFromBizThread(new ReleaseBufferTask(pool, buf));
    }
}

@SuppressWarnings("FieldMayBeFinal")
class ReleaseBufferTask implements Runnable {
    private SimpleByteBufferPool pool;
    private ByteBuffer buffer;

    public ReleaseBufferTask(SimpleByteBufferPool pool, ByteBuffer buffer) {
        this.pool = pool;
        this.buffer = buffer;
    }

    @Override
    public void run() {
        pool.release(buffer);
    }
}

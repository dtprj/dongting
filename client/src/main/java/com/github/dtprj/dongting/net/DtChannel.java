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

import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.codec.PbParser;
import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author huangli
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class DtChannel extends PbCallback<Object> {
    private static final DtLog log = DtLogs.getLogger(DtChannel.class);

    private final NioStatus nioStatus;
    private final NioConfig nioConfig;
    private final WorkerStatus workerStatus;
    private final SocketChannel channel;
    private final ChannelContext channelContext;
    private final DecodeContext decodeContext;
    private final RespWriter respWriter;
    private Peer peer;

    private final int channelIndexInWorker;
    int seq = 1;

    // read status
    private final PbParser parser;
    private ReadFrame frame;
    private boolean readBody;
    private WriteData requestForResp;
    private ReqProcessor processorForRequest;
    private int currentReadFrameSize;
    private Decoder<?> currentDecoder;

    private boolean running = true;

    private final IoChannelQueue subQueue;

    private boolean closed;

    public DtChannel(NioStatus nioStatus, WorkerStatus workerStatus, NioConfig nioConfig,
                     SocketChannel socketChannel, int channelIndexInWorker) throws IOException {
        this.nioStatus = nioStatus;
        this.channel = socketChannel;
        this.nioConfig = nioConfig;
        this.workerStatus = workerStatus;
        this.channelIndexInWorker = channelIndexInWorker;
        this.parser = PbParser.multiParser(this, nioConfig.getMaxFrameSize());

        this.respWriter = new RespWriter(workerStatus.getIoQueue(), workerStatus.getWakeupRunnable(), this);

        ChannelContext channelContext = new ChannelContext();
        channelContext.setChannel(socketChannel);
        channelContext.setRemoteAddr(socketChannel.getRemoteAddress());
        channelContext.setLocalAddr(socketChannel.getLocalAddress());
        channelContext.setRespWriter(respWriter);
        this.channelContext = channelContext;

        this.decodeContext = new DecodeContext();
        this.decodeContext.setHeapPool(workerStatus.getHeapPool());

        this.subQueue = new IoChannelQueue(nioConfig, workerStatus, this, workerStatus.getHeapPool());
    }

    public void afterRead(boolean running, ByteBuffer buf) {
        if (!running) {
            this.running = false;
        }
        parser.parse(buf);
    }

    @Override
    protected void begin(int len) {
        this.currentReadFrameSize = len;
        frame = new ReadFrame();
        readBody = false;
        requestForResp = null;
        processorForRequest = null;
        if (currentDecoder != null) {
            BugLog.getLog().error("currentDecoder is not null");
            currentDecoder = null;
        }
    }

    @Override
    protected void end(boolean success) {
        try {
            if (!success) {
                return;
            }

            if (requestForResp == null && processorForRequest == null) {
                // empty body
                if (!initRelatedDataForFrame()) {
                    return;
                }
                if (requestForResp == null && processorForRequest == null) {
                    return;
                }
                if (currentDecoder != null) {
                    Object o = currentDecoder.decode(decodeContext, SimpleByteBufferPool.EMPTY_BUFFER, 0, 0);
                    frame.setBody(o);
                }
            }
        } finally {
            resetDecoder();
        }

        if (frame.getFrameType() == FrameType.TYPE_RESP) {
            processIncomingResponse(frame, requestForResp);
        } else {
            // req or one way
            processIncomingRequest(frame, processorForRequest, workerStatus.getTs());
        }
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
        }
        return true;
    }

    @Override
    public boolean readFix32(int index, int value) {
        if (this.readBody) {
            throw new PbException("body has read");
        }
        if (index == Frame.IDX_SEQ) {
            frame.setSeq(value);
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        if (this.readBody) {
            throw new PbException("body has read");
        }
        if (index == Frame.IDX_TIMOUT) {
            frame.setTimeout(value);
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (this.readBody) {
            throw new PbException("body has read");
        }
        switch (index) {
            case Frame.IDX_MSG: {
                this.frame.setMsg(parseUTF8(buf, fieldLen, currentPos));
                return true;
            }
            case Frame.IDX_EXTRA: {
                this.frame.setExtra(parseBytes(buf, fieldLen, currentPos));
                return true;
            }
            case Frame.IDX_BODY: {
                boolean end = buf.remaining() >= fieldLen - currentPos;
                try {
                    if (currentPos == 0 && currentDecoder != null) {
                        throw new IllegalStateException("currentDecoder is not null");
                    }
                    return readBody(buf, fieldLen, currentPos, end);
                } finally {
                    if (end) {
                        resetDecoder();
                    }
                }
            }
            default:
                return true;
        }

    }

    private void resetDecoder() {
        if (currentDecoder != null) {
            try {
                currentDecoder.finish(decodeContext);
            } finally {
                currentDecoder = null;
            }
        }
        decodeContext.reset();
    }

    private boolean readBody(ByteBuffer buf, int fieldLen, int currentPos, boolean end) {
        ReadFrame frame = this.frame;
        if (frame.getCommand() <= 0) {
            throw new NetException("command invalid :" + frame.getCommand());
        }
        // the body field should encode as last field
        if (!initRelatedDataForFrame()) {
            return false;
        }
        if (currentDecoder == null) {
            return false;
        }

        try {
            Object o = currentDecoder.decode(decodeContext, buf, fieldLen, currentPos);
            if (end) {
                frame.setBody(o);
                // so if the body is not last field, exception throws
                readBody = true;
            }
            return true;
        } catch (Throwable e) {
            processIoDecodeFail(e);
            return false;
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean initRelatedDataForFrame() {
        ReadFrame frame = this.frame;
        if (frame.getFrameType() == FrameType.TYPE_RESP) {
            WriteData requestForResp = this.requestForResp;
            if (requestForResp == null) {
                requestForResp = this.workerStatus.getPendingRequests().remove(BitUtil.toLong(channelIndexInWorker, frame.getSeq()));
                if (requestForResp == null) {
                    log.info("pending request not found. channel={}, resp={}", channel, frame);
                    return false;
                } else {
                    this.requestForResp = requestForResp;
                }
            }
            if (currentDecoder == null) {
                currentDecoder = requestForResp.getRespDecoder();
            }
        } else {
            // req or one way
            if (!running) {
                writeErrorInIoThread(frame, CmdCodes.STOPPING, null);
                return false;
            }
            ReqProcessor processorForRequest = this.processorForRequest;
            if (processorForRequest == null) {
                processorForRequest = nioStatus.getProcessor(frame.getCommand());
                if (processorForRequest == null) {
                    log.warn("command {} is not support", frame.getCommand());
                    writeErrorInIoThread(frame, CmdCodes.COMMAND_NOT_SUPPORT, null);
                    return false;
                }
                if (processorForRequest.getExecutor() != null) {
                    // TODO can we eliminate this CAS operation?
                    AtomicLong inReqBytes = nioStatus.getInReqBytes();
                    if (inReqBytes != null) {
                        long bytesAfterAdd = inReqBytes.addAndGet(this.currentReadFrameSize);
                        if (bytesAfterAdd > nioConfig.getMaxInBytes()) {
                            log.debug("pendingBytes({})>maxInBytes({}), write response code FLOW_CONTROL to client",
                                    bytesAfterAdd, nioConfig.getMaxInBytes());
                            writeErrorInIoThread(frame, CmdCodes.FLOW_CONTROL,
                                    "max incoming request bytes: " + nioConfig.getMaxInBytes());
                            inReqBytes.addAndGet(-currentReadFrameSize);
                            return false;
                        }
                    }
                }
                this.processorForRequest = processorForRequest;
            }
            if (currentDecoder == null) {
                currentDecoder = processorForRequest.createDecoder(frame.getCommand());
            }
        }
        return true;
    }

    private void processIoDecodeFail(Throwable e) {
        if (log.isDebugEnabled()) {
            log.debug("decode fail. {} {}", channel, e.toString());
        }
        if (frame.getFrameType() == FrameType.TYPE_RESP) {
            if (requestForResp != null) {
                requestForResp.callFail(false, e);
            }
        } else {
            // req or one way
            log.warn("decode fail in io thread", e);
            writeErrorInIoThread(frame, CmdCodes.BIZ_ERROR, "decode fail: " + e.toString());
        }
    }

    private void processIncomingResponse(ReadFrame resp, WriteData wo) {
        WriteFrame req = wo.getData();
        if (resp.getCommand() != req.getCommand()) {
            wo.callFail(false, new NetException("command not match"));
            return;
        }
        wo.callSuccess(resp);
    }

    private void processIncomingRequest(ReadFrame req, ReqProcessor p, Timestamp roundTime) {
        NioStatus nioStatus = this.nioStatus;
        ReqContext reqContext = new ReqContext();
        reqContext.setTimeout(new DtTime(roundTime, req.getTimeout(), TimeUnit.NANOSECONDS));
        if (p.getExecutor() == null) {
            if (timeout(req, channelContext, reqContext, roundTime)) {
                return;
            }
            WriteFrame resp;
            try {
                resp = p.process(req, channelContext, reqContext);
            } catch (NetCodeException e) {
                log.warn("ReqProcessor.process fail, command={}, code={}, msg={}",
                        req.getCommand(), e.getCode(), e.getMessage());
                writeErrorInIoThread(req, e.getCode(), e.getMessage(), reqContext.getTimeout());
                return;
            } catch (Throwable e) {
                log.warn("ReqProcessor.process fail", e);
                writeErrorInIoThread(req, CmdCodes.BIZ_ERROR, e.toString(), reqContext.getTimeout());
                return;
            }
            if (resp != null) {
                resp.setCommand(req.getCommand());
                resp.setFrameType(FrameType.TYPE_RESP);
                resp.setSeq(req.getSeq());
                subQueue.enqueue(new WriteData(this, resp, reqContext.getTimeout()));
            }
        } else {
            AtomicLong bytes = nioStatus.getInReqBytes();
            int currentReadFrameSize = this.currentReadFrameSize;
            try {
                // TODO use custom thread pool?
                p.getExecutor().execute(new ProcessInBizThreadTask(
                        req, p, currentReadFrameSize, this, bytes, reqContext));
            } catch (RejectedExecutionException e) {
                log.debug("catch RejectedExecutionException, write response code FLOW_CONTROL to client, maxInRequests={}",
                        nioConfig.getMaxInRequests());
                writeErrorInIoThread(req, CmdCodes.FLOW_CONTROL,
                        "max incoming request: " + nioConfig.getMaxInRequests(), reqContext.getTimeout());
                if (bytes != null) {
                    bytes.addAndGet(-currentReadFrameSize);
                }
            }
        }
    }

    static boolean timeout(ReadFrame rf, ChannelContext channelContext, ReqContext reqContext, Timestamp ts) {
        DtTime t = reqContext.getTimeout();
        boolean timeout = ts == null ? t.isTimeout() : t.isTimeout(ts);
        if (timeout) {
            String type = FrameType.toStr(rf.getFrameType());
            log.debug("drop timeout {}, remote={}, seq={}, timeout={}ms", type,
                    channelContext.getRemoteAddr(), rf.getSeq(), t.getTimeout(TimeUnit.MILLISECONDS));
            return true;
        } else {
            return false;
        }
    }

    private void writeErrorInIoThread(Frame req, int code, String msg) {
        writeErrorInIoThread(req, code, msg, new DtTime(10, TimeUnit.SECONDS));
    }

    private void writeErrorInIoThread(Frame req, int code, String msg, DtTime timeout) {
        if (req.getFrameType() == FrameType.TYPE_ONE_WAY) {
            return;
        }
        EmptyBodyRespFrame resp = new EmptyBodyRespFrame(code);
        resp.setCommand(req.getCommand());
        resp.setFrameType(FrameType.TYPE_RESP);
        resp.setSeq(req.getSeq());
        resp.setMsg(msg);
        subQueue.enqueue(new WriteData(this, resp, timeout));
    }

    public int getAndIncSeq() {
        return seq++;
    }

    public int getChannelIndexInWorker() {
        return channelIndexInWorker;
    }

    public IoChannelQueue getSubQueue() {
        return subQueue;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public boolean isClosed() {
        return closed;
    }

    public ChannelContext getProcessContext() {
        return channelContext;
    }

    public void close() {
        if (closed) {
            return;
        }
        parser.reset();
        closed = true;
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

    public RespWriter getRespWriter() {
        return respWriter;
    }
}

@SuppressWarnings({"rawtypes", "unchecked"})
class ProcessInBizThreadTask implements Runnable {
    private static final DtLog log = DtLogs.getLogger(ProcessInBizThreadTask.class);
    private final ReadFrame req;
    private final ReqProcessor processor;
    private final int frameSize;
    private final DtChannel dtc;
    private final AtomicLong inBytes;
    private final ReqContext reqContext;

    ProcessInBizThreadTask(ReadFrame req, ReqProcessor processor,
                           int frameSize, DtChannel dtc, AtomicLong inBytes, ReqContext reqContext) {
        this.req = req;
        this.processor = processor;
        this.frameSize = frameSize;
        this.dtc = dtc;
        this.inBytes = inBytes;
        this.reqContext = reqContext;
    }

    @Override
    public void run() {
        WriteFrame resp = null;
        ReadFrame req = this.req;
        DtChannel dtc = this.dtc;
        try {
            if (DtChannel.timeout(req, dtc.getProcessContext(), reqContext, null)) {
                return;
            }
            resp = processor.process(req, dtc.getProcessContext(), reqContext);
        } catch (NetCodeException e) {
            log.warn("ReqProcessor.process fail, command={}, code={}, msg={}", req.getCommand(), e.getCode(), e.getMessage());
            if (req.getFrameType() == FrameType.TYPE_REQ) {
                resp = new EmptyBodyRespFrame(e.getCode());
                resp.setMsg(e.toString());
            }
        } catch (Throwable e) {
            log.warn("ReqProcessor.process fail, command={}", req.getCommand(), e);
            if (req.getFrameType() == FrameType.TYPE_REQ) {
                resp = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
                resp.setMsg(e.toString());
            }
        } finally {
            if (inBytes != null) {
                inBytes.addAndGet(-frameSize);
            }
        }
        if (resp != null) {
            dtc.getRespWriter().writeRespInBizThreads(req, resp, reqContext.getTimeout());
        }
    }
}
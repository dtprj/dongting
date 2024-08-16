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
import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class DtChannelImpl extends PbCallback<Object> implements DtChannel {
    private static final DtLog log = DtLogs.getLogger(DtChannelImpl.class);

    private final NioStatus nioStatus;
    private final NioConfig nioConfig;
    final WorkerStatus workerStatus;
    private final SocketChannel channel;
    private final DecodeContext decodeContext;
    private final RespWriter respWriter;
    private final Peer peer;
    private final SocketAddress remoteAddr;
    private final SocketAddress localAddr;

    private final int channelIndexInWorker;
    int seq = 1;

    // read status
    private final MultiParser parser;
    private ReadPacket packet;
    private boolean readBody;
    private WriteData requestForResp;
    private ReqProcessor processorForRequest;
    private int currentReadPacketSize;
    private Decoder<?> currentDecoder;
    private boolean flowControl;

    private boolean running = true;

    private final IoChannelQueue subQueue;

    private boolean closed;

    boolean listenerOnConnectedCalled;


    public DtChannelImpl(NioStatus nioStatus, WorkerStatus workerStatus, NioConfig nioConfig, Peer peer,
                         SocketChannel socketChannel, int channelIndexInWorker) throws IOException {
        this.nioStatus = nioStatus;
        this.channel = socketChannel;
        this.nioConfig = nioConfig;
        this.workerStatus = workerStatus;
        this.channelIndexInWorker = channelIndexInWorker;
        this.peer = peer;
        this.parser = new MultiParser(this, nioConfig.getMaxPacketSize());

        this.respWriter = new RespWriter(workerStatus.getIoQueue(), workerStatus.getWakeupRunnable(), this);

        this.remoteAddr = channel.getRemoteAddress();
        this.localAddr = channel.getLocalAddress();

        this.decodeContext = new DecodeContext(workerStatus.getHeapPool());

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
        this.currentReadPacketSize = len;
        packet = new ReadPacket();
        readBody = false;
        requestForResp = null;
        processorForRequest = null;
        flowControl = false;
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
                if (!initRelatedDataForPacket()) {
                    return;
                }
                if (requestForResp == null && processorForRequest == null) {
                    return;
                }
                if (currentDecoder != null) {
                    packet.body = currentDecoder.decode(decodeContext, SimpleByteBufferPool.EMPTY_BUFFER, 0, 0);
                }
            }
        } finally {
            if (currentDecoder != null) {
                try {
                    currentDecoder.finish(decodeContext);
                    decodeContext.reset();
                } finally {
                    currentDecoder = null;
                }
            }
        }

        if (packet.getPacketType() == PacketType.TYPE_RESP) {
            processIncomingResponse(packet, requestForResp);
        } else {
            // req or one way
            processIncomingRequest(packet, processorForRequest, workerStatus.getTs());
        }
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        if (readBody) {
            throw new PbException("body has read");
        }
        switch (index) {
            case Packet.IDX_TYPE:
                packet.packetType = (int) value;
                break;
            case Packet.IDX_COMMAND:
                packet.command = (int) value;
                break;
            case Packet.IDX_RESP_CODE:
                packet.respCode = (int) value;
                break;
        }
        return true;
    }

    @Override
    public boolean readFix32(int index, int value) {
        if (this.readBody) {
            throw new PbException("body has read");
        }
        if (index == Packet.IDX_SEQ) {
            packet.seq = value;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        if (this.readBody) {
            throw new PbException("body has read");
        }
        if (index == Packet.IDX_TIMOUT) {
            packet.timeout = value;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (this.readBody) {
            throw new PbException("body has read");
        }
        switch (index) {
            case Packet.IDX_MSG: {
                this.packet.msg = parseUTF8(buf, fieldLen, currentPos);
                return true;
            }
            case Packet.IDX_EXTRA: {
                this.packet.extra = parseBytes(buf, fieldLen, currentPos);
                return true;
            }
            case Packet.IDX_BODY: {
                if (currentPos == 0 && currentDecoder != null) {
                    throw new IllegalStateException("currentDecoder is not null");
                }
                boolean end = buf.remaining() >= fieldLen - currentPos;
                return readBody(buf, fieldLen, currentPos, end);
            }
            default:
                return true;
        }

    }

    private boolean readBody(ByteBuffer buf, int fieldLen, int currentPos, boolean end) {
        ReadPacket packet = this.packet;
        if (packet.getCommand() <= 0) {
            throw new NetException("command invalid :" + packet.getCommand());
        }
        // the body field should encode as last field
        if (!initRelatedDataForPacket()) {
            return false;
        }
        if (currentDecoder == null) {
            return false;
        }

        try {
            Object o = currentDecoder.decode(decodeContext, buf, fieldLen, currentPos);
            if (end) {
                packet.body = o;
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
    private boolean initRelatedDataForPacket() {
        ReadPacket packet = this.packet;
        if (packet.getPacketType() == PacketType.TYPE_RESP) {
            WriteData requestForResp = this.requestForResp;
            if (requestForResp == null) {
                requestForResp = this.workerStatus.getPendingRequests().remove(BitUtil.toLong(channelIndexInWorker, packet.getSeq()));
                if (requestForResp == null) {
                    log.info("pending request not found. channel={}, resp={}", channel, packet);
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
                writeErrorInIoThread(packet, CmdCodes.STOPPING, null);
                return false;
            }
            if (processorForRequest == null) {
                processorForRequest = nioStatus.getProcessor(packet.getCommand());
                if (processorForRequest == null) {
                    log.warn("command {} is not support", packet.getCommand());
                    writeErrorInIoThread(packet, CmdCodes.COMMAND_NOT_SUPPORT, null);
                    return false;
                }
            }
            if (currentDecoder == null) {
                currentDecoder = processorForRequest.createDecoder(packet.getCommand());
            }
        }
        return true;
    }

    private boolean flowControlCheck(int bytes) {
        int maxReq = nioConfig.getMaxInRequests();
        long maxBytes = nioConfig.getMaxInBytes();
        if (maxReq <= 0 && maxBytes <= 0) {
            return true;
        }
        nioStatus.pendingLock.lock();
        try {
            if (maxReq > 0 && nioStatus.pendingRequests + 1 > maxReq) {
                log.debug("pendingRequests({})>maxInRequests({}), write response code FLOW_CONTROL to client",
                        nioStatus.pendingRequests + 1, nioConfig.getMaxInRequests());
                writeErrorInIoThread(packet, CmdCodes.FLOW_CONTROL,
                        "max incoming request: " + nioConfig.getMaxInRequests());
                return false;

            }
            if (maxBytes > 0 && nioStatus.pendingBytes + bytes > maxBytes) {
                log.debug("pendingBytes({})>maxInBytes({}), write response code FLOW_CONTROL to client",
                        nioStatus.pendingBytes + bytes, nioConfig.getMaxInBytes());
                writeErrorInIoThread(packet, CmdCodes.FLOW_CONTROL,
                        "max incoming request bytes: " + nioConfig.getMaxInBytes());
                return false;
            }
            flowControl = true;
            nioStatus.pendingRequests++;
            nioStatus.pendingBytes += bytes;
        } finally {
            nioStatus.pendingLock.unlock();
        }
        return true;
    }

    void releasePending(int bytes) {
        nioStatus.pendingLock.lock();
        try {
            nioStatus.pendingRequests--;
            nioStatus.pendingBytes -= bytes;
        } finally {
            nioStatus.pendingLock.unlock();
        }
    }

    private void processIoDecodeFail(Throwable e) {
        if (log.isDebugEnabled()) {
            log.debug("decode fail. {} {}", channel, e.toString());
        }
        if (packet.packetType == PacketType.TYPE_RESP) {
            if (requestForResp != null) {
                requestForResp.callFail(false, e);
            }
        } else if (packet.packetType == PacketType.TYPE_REQ || packet.packetType == PacketType.TYPE_ONE_WAY) {
            if (packet.command > 0) {
                log.warn("decode fail in io thread", e);
                writeErrorInIoThread(packet, CmdCodes.BIZ_ERROR, "decode fail: " + e.toString());
            }
        }
    }

    private void processIncomingResponse(ReadPacket resp, WriteData wo) {
        WritePacket req = wo.getData();
        if (resp.getCommand() != req.getCommand()) {
            wo.callFail(false, new NetException("command not match"));
            return;
        }
        wo.callSuccess(resp);
    }

    private void processIncomingRequest(ReadPacket req, ReqProcessor p, Timestamp roundTime) {
        if (!flowControlCheck(currentReadPacketSize)) {
            return;
        }
        ReqContext reqContext = new ReqContext(this, new DtTime(roundTime, req.getTimeout(), TimeUnit.NANOSECONDS));
        if (p.executor == null) {
            if (timeout(req, reqContext, roundTime)) {
                return;
            }
            WritePacket resp;
            try {
                resp = p.process(req, reqContext);
            } catch (NetCodeException e) {
                log.warn("ReqProcessor.process fail, command={}, code={}, msg={}",
                        req.getCommand(), e.getCode(), e.getMessage());
                writeErrorInIoThread(req, e.getCode(), e.getMessage(), reqContext.getTimeout());
                return;
            } catch (Throwable e) {
                log.warn("ReqProcessor.process fail", e);
                writeErrorInIoThread(req, CmdCodes.BIZ_ERROR, e.toString(), reqContext.getTimeout());
                return;
            } finally {
                if (flowControl) {
                    releasePending(currentReadPacketSize);
                }
            }
            if (resp != null) {
                resp.setCommand(req.getCommand());
                resp.setPacketType(PacketType.TYPE_RESP);
                resp.setSeq(req.getSeq());
                subQueue.enqueue(new WriteData(this, resp, reqContext.getTimeout()));
            }
        } else {
            try {
                p.executor.execute(new ProcessInBizThreadTask(
                        req, p, currentReadPacketSize, this, flowControl, reqContext));
            } catch (RejectedExecutionException e) {
                log.debug("catch RejectedExecutionException, write response code FLOW_CONTROL to client, maxInRequests={}",
                        nioConfig.getMaxInRequests());
                writeErrorInIoThread(req, CmdCodes.FLOW_CONTROL,
                        "max incoming request: " + nioConfig.getMaxInRequests(), reqContext.getTimeout());
                if (flowControl) {
                    releasePending(currentReadPacketSize);
                }
            }
        }
    }

    static boolean timeout(ReadPacket rf, ReqContext reqContext, Timestamp ts) {
        DtTime t = reqContext.getTimeout();
        boolean timeout = ts == null ? t.isTimeout() : t.isTimeout(ts);
        if (timeout) {
            String type = PacketType.toStr(rf.getPacketType());
            log.debug("drop timeout {}, remote={}, seq={}, timeout={}ms", type,
                    reqContext.getDtChannel().getRemoteAddr(), rf.getSeq(), t.getTimeout(TimeUnit.MILLISECONDS));
            return true;
        } else {
            return false;
        }
    }

    private void writeErrorInIoThread(Packet req, int code, String msg) {
        writeErrorInIoThread(req, code, msg, new DtTime(10, TimeUnit.SECONDS));
    }

    private void writeErrorInIoThread(Packet req, int code, String msg, DtTime timeout) {
        if (req.getPacketType() == PacketType.TYPE_ONE_WAY) {
            return;
        }
        EmptyBodyRespPacket resp = new EmptyBodyRespPacket(code);
        resp.setCommand(req.getCommand());
        resp.setPacketType(PacketType.TYPE_RESP);
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

    @Override
    public SocketChannel getChannel() {
        return channel;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public SocketAddress getRemoteAddr() {
        return remoteAddr;
    }

    @Override
    public SocketAddress getLocalAddr() {
        return localAddr;
    }

    public void close() {
        if (closed) {
            return;
        }
        parser.reset();
        closed = true;
    }

    public Peer getPeer() {
        return peer;
    }

    // for unit test
    MultiParser getParser() {
        return parser;
    }

    // for unit test
    ReadPacket getPacket() {
        return packet;
    }

    @Override
    public RespWriter getRespWriter() {
        return respWriter;
    }
}

@SuppressWarnings({"rawtypes", "unchecked"})
class ProcessInBizThreadTask implements Runnable {
    private static final DtLog log = DtLogs.getLogger(ProcessInBizThreadTask.class);
    private final ReadPacket req;
    private final ReqProcessor processor;
    private final int packetSize;
    private final DtChannelImpl dtc;
    private final boolean flowControl;
    private final ReqContext reqContext;

    ProcessInBizThreadTask(ReadPacket req, ReqProcessor processor,
                           int packetSize, DtChannelImpl dtc, boolean flowControl, ReqContext reqContext) {
        this.req = req;
        this.processor = processor;
        this.packetSize = packetSize;
        this.dtc = dtc;
        this.flowControl = flowControl;
        this.reqContext = reqContext;
    }

    @Override
    public void run() {
        WritePacket resp = null;
        ReadPacket req = this.req;
        DtChannelImpl dtc = this.dtc;
        try {
            if (DtChannelImpl.timeout(req, reqContext, null)) {
                return;
            }
            resp = processor.process(req, reqContext);
        } catch (NetCodeException e) {
            log.warn("ReqProcessor.process fail, command={}, code={}, msg={}", req.getCommand(), e.getCode(), e.getMessage());
            if (req.getPacketType() == PacketType.TYPE_REQ) {
                resp = new EmptyBodyRespPacket(e.getCode());
                resp.setMsg(e.toString());
            }
        } catch (Throwable e) {
            log.warn("ReqProcessor.process fail, command={}", req.getCommand(), e);
            if (req.getPacketType() == PacketType.TYPE_REQ) {
                resp = new EmptyBodyRespPacket(CmdCodes.BIZ_ERROR);
                resp.setMsg(e.toString());
            }
        } finally {
            if (flowControl) {
                dtc.releasePending(packetSize);
            }
        }
        if (resp != null) {
            dtc.getRespWriter().writeRespInBizThreads(req, resp, reqContext.getTimeout());
        }
    }
}
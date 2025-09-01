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
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.common.BitUtil;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class DtChannelImpl extends PbCallback<Object> implements DtChannel {
    private static final DtLog log = DtLogs.getLogger(DtChannelImpl.class);

    UUID remoteUuid;

    private final NioStatus nioStatus;
    private final NioConfig nioConfig;
    final WorkerStatus workerStatus;
    private final SocketChannel channel;
    private final DecodeContext decodeContext;
    final Peer peer; // null in server side
    private final SocketAddress remoteAddr;
    private final SocketAddress localAddr;

    final int channelIndexInWorker;
    final long createTimeNanos;

    int seq = 1;

    private final MultiParser parser;

    // read status
    private ReadPacket packet;
    private boolean readBody;
    private WriteData requestForResp;
    private ReqProcessor processorForRequest;
    private int currentReadPacketSize;
    private DecoderCallback currentDecoderCallback;

    final IoChannelQueue subQueue;

    private boolean running = true;
    private boolean closed;
    boolean handshake;
    boolean listenerOnConnectedCalled;

    long lastActiveTimeNanos;

    public DtChannelImpl(NioStatus nioStatus, WorkerStatus workerStatus, NioConfig nioConfig, Peer peer,
                         SocketChannel socketChannel, int channelIndexInWorker) throws IOException {
        this.nioStatus = nioStatus;
        this.channel = socketChannel;
        this.nioConfig = nioConfig;
        this.workerStatus = workerStatus;
        this.channelIndexInWorker = channelIndexInWorker;
        this.peer = peer;
        this.createTimeNanos = workerStatus.ts.getNanoTime();

        this.decodeContext = nioConfig.decodeContextFactory.get();
        this.decodeContext.setHeapPool(workerStatus.heapPool);

        this.parser = new MultiParser(decodeContext, this, nioConfig.maxPacketSize);

        this.remoteAddr = channel.getRemoteAddress();
        this.localAddr = channel.getLocalAddress();

        this.subQueue = new IoChannelQueue(nioConfig, workerStatus, this, workerStatus.heapPool);
        this.lastActiveTimeNanos = workerStatus.ts.getNanoTime();
    }

    public void afterRead(boolean running, ByteBuffer buf) {
        if (!running) {
            this.running = false;
        }
        parser.parse(buf);
    }

    @Override
    public Object getResult() {
        return null;
    }

    @Override
    protected void begin(int len) {
        this.currentReadPacketSize = len;
        packet = new ReadPacket();
        readBody = false;
        requestForResp = null;
        processorForRequest = null;
        currentDecoderCallback = null;
    }

    @Override
    protected boolean end(boolean success) {
        if (!success) {
            return false;
        }

        if (!handshake && peer == null) { // peer is null is server side
            if (packet.command != Commands.CMD_HANDSHAKE || packet.packetType != PacketType.TYPE_REQ) {
                throw new NetException("first command is not handshake, command=" + packet.command);
            }
        }

        if (requestForResp == null && processorForRequest == null) {
            // empty body
            if (!readBody(SimpleByteBufferPool.EMPTY_BUFFER, 0, 0, true)) {
                return false;
            }
        }
        currentDecoderCallback = null;

        if (packet.packetType == PacketType.TYPE_RESP) {
            processIncomingResponse(packet, requestForResp);
        } else {
            // req or one way
            processIncomingRequest(packet, processorForRequest, workerStatus.ts);
        }
        this.lastActiveTimeNanos = workerStatus.ts.getNanoTime();
        return true;
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
            case Packet.IDX_BIZ_CODE:
                packet.bizCode = (int) value;
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
        if (index == Packet.IDX_TIMEOUT) {
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
                if (currentPos == 0 && currentDecoderCallback != null) {
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
        if (packet.command <= 0) {
            throw new NetException("command invalid :" + packet.command);
        }
        // the body field should encode as last field
        if (!initRelatedDataForPacket()) {
            return false;
        }
        if (currentDecoderCallback == null) {
            return false;
        }

        try {
            packet.body = parseNested(buf, fieldLen, currentPos, currentDecoderCallback);
        } catch (RuntimeException | Error e) {
            if (packet.packetType == PacketType.TYPE_RESP) {
                if (requestForResp != null) {
                    requestForResp.callFail(false, e);
                }
            }
            throw e;
        } finally {
            if (end) {
                // so if the body is not last field, exception throws
                readBody = true;
                if (context.createOrGetNestedDecoder().shouldSkip()) {
                    log.warn("skip parse, command={}", packet.command);
                }
            }
        }
        return true;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean initRelatedDataForPacket() {
        ReadPacket packet = this.packet;
        if (packet.packetType == PacketType.TYPE_RESP) {
            WriteData requestForResp = this.requestForResp;
            if (requestForResp == null) {
                requestForResp = this.workerStatus.pendingRequests.remove(BitUtil.toLong(channelIndexInWorker, packet.seq));
                if (requestForResp == null) {
                    log.info("pending request not found. channel={}, resp={}", channel, packet);
                    return false;
                } else {
                    this.requestForResp = requestForResp;
                }
            }
            if (currentDecoderCallback == null) {
                currentDecoderCallback = requestForResp.respDecoderCallback.apply(decodeContext.createOrGetNestedContext());
            }
        } else {
            // req or one way
            if (!running) {
                writeErrorInIoThread(packet, CmdCodes.STOPPING, null);
                return false;
            }
            if (processorForRequest == null) {
                processorForRequest = nioStatus.getProcessor(packet.command);
                if (processorForRequest == null) {
                    log.warn("command {} is not support", packet.command);
                    if (packet.packetType == PacketType.TYPE_REQ) {
                        writeErrorInIoThread(packet, CmdCodes.COMMAND_NOT_SUPPORT, null);
                    }
                    return false;
                }
            }
            if (currentDecoderCallback == null) {
                currentDecoderCallback = processorForRequest.createDecoderCallback(packet.command, decodeContext.createOrGetNestedContext());
            }
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

    private void processIncomingResponse(ReadPacket resp, WriteData wo) {
        WritePacket req = wo.data;
        if (resp.command != req.command) {
            wo.callFail(false, new NetException("command not match"));
            return;
        }
        wo.callSuccess(resp);
    }

    private void processIncomingRequest(ReadPacket req, ReqProcessor p, Timestamp roundTime) {
        int maxReq = nioConfig.maxInRequests;
        long maxBytes = nioConfig.maxInBytes;
        boolean flowControl;
        if (maxReq <= 0 && maxBytes <= 0) {
            flowControl = false;
        } else {
            nioStatus.pendingLock.lock();
            try {
                if (maxReq > 0 && nioStatus.pendingRequests + 1 > maxReq) {
                    log.debug("pendingRequests({})>maxInRequests({}), write response code FLOW_CONTROL to client",
                            nioStatus.pendingRequests + 1, nioConfig.maxInRequests);
                    writeErrorInIoThread(packet, CmdCodes.FLOW_CONTROL,
                            "max incoming request: " + nioConfig.maxInRequests);
                    return;
                }
                if (maxBytes > 0 && nioStatus.pendingBytes + currentReadPacketSize > maxBytes) {
                    log.debug("pendingBytes({})>maxInBytes({}), write response code FLOW_CONTROL to client",
                            nioStatus.pendingBytes + currentReadPacketSize, nioConfig.maxInBytes);
                    writeErrorInIoThread(packet, CmdCodes.FLOW_CONTROL,
                            "max incoming request bytes: " + nioConfig.maxInBytes);
                    return;
                }
                nioStatus.pendingRequests++;
                nioStatus.pendingBytes += currentReadPacketSize;
            } finally {
                nioStatus.pendingLock.unlock();
            }
            flowControl = true;
        }

        ReqContext reqContext = new ReqContext(this, req, new DtTime(roundTime, req.timeout, TimeUnit.NANOSECONDS));
        if (p.executor == null) {
            if (timeout(req, reqContext, roundTime)) {
                return;
            }
            WritePacket resp;
            try {
                resp = p.process(req, reqContext);
            } catch (NetCodeException e) {
                log.warn("ReqProcessor.process fail, command={}, code={}, msg={}",
                        req.command, e.getCode(), e.getMessage());
                writeErrorInIoThread(req, e.getCode(), e.getMessage(), reqContext.getTimeout());
                return;
            } catch (Throwable e) {
                log.warn("ReqProcessor.process fail", e);
                writeErrorInIoThread(req, CmdCodes.SYS_ERROR, e.toString(), reqContext.getTimeout());
                return;
            } finally {
                if (flowControl) {
                    releasePending(currentReadPacketSize);
                }
            }
            if (resp != null) {
                resp.command = req.command;
                resp.packetType = PacketType.TYPE_RESP;
                resp.seq = req.seq;
                subQueue.enqueue(new WriteData(this, resp, reqContext.getTimeout()));
            }
        } else {
            try {
                p.executor.execute(new ProcessInBizThreadTask(
                        req, p, currentReadPacketSize, this, flowControl, reqContext));
            } catch (RejectedExecutionException e) {
                log.debug("catch RejectedExecutionException, write response code FLOW_CONTROL to client, maxInRequests={}",
                        nioConfig.maxInRequests);
                writeErrorInIoThread(req, CmdCodes.FLOW_CONTROL,
                        "max incoming request: " + nioConfig.maxInRequests, reqContext.getTimeout());
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
            String type = PacketType.toStr(rf.packetType);
            log.debug("drop timeout {}, remote={}, seq={}, timeout={}ms", type,
                    reqContext.getDtChannel().getRemoteAddr(), rf.seq, t.getTimeout(TimeUnit.MILLISECONDS));
            return true;
        } else {
            return false;
        }
    }

    protected void writeErrorInIoThread(Packet req, int code, String msg) {
        writeErrorInIoThread(req, code, msg, new DtTime(10, TimeUnit.SECONDS));
    }

    private void writeErrorInIoThread(Packet req, int code, String msg, DtTime timeout) {
        if (req.packetType == PacketType.TYPE_ONE_WAY) {
            return;
        }
        EmptyBodyRespPacket resp = new EmptyBodyRespPacket(code);
        resp.command = req.command;
        resp.packetType = PacketType.TYPE_RESP;
        resp.seq = req.seq;
        resp.msg = msg;
        subQueue.enqueue(new WriteData(this, resp, timeout));
    }

    public int getAndIncSeq() {
        int c = seq++;
        if (c < 0) {
            seq = 1;
            c = 1;
        }
        return c;
    }

    @Override
    public SocketChannel getChannel() {
        return channel;
    }

    @Override
    public Peer getPeer() {
        return peer;
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
        closed = true;
        try {
            decodeContext.reset(parser.getParser());
        } catch (Exception e) {
            log.error("channel close error", e);
        }
    }

    // for unit test
    MultiParser getParser() {
        return parser;
    }

    // for unit test
    ReadPacket getPacket() {
        return packet;
    }

    public long getLastActiveTimeNanos() {
        return lastActiveTimeNanos;
    }

    @Override
    public NioNet getOwner() {
        return workerStatus.worker.owner;
    }

    @Override
    public String toString() {
        return "dtc : " + channel.toString();
    }

    @Override
    public UUID getRemoteUuid() {
        return remoteUuid;
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
            log.warn("ReqProcessor.process fail, command={}, code={}, msg={}", req.command, e.getCode(), e.getMessage());
            if (req.packetType == PacketType.TYPE_REQ) {
                resp = new EmptyBodyRespPacket(e.getCode());
                resp.msg = e.toString();
            }
        } catch (Throwable e) {
            log.warn("ReqProcessor.process fail, command={}", req.command, e);
            if (req.packetType == PacketType.TYPE_REQ) {
                resp = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
                resp.msg = e.toString();
            }
        } finally {
            if (flowControl) {
                dtc.releasePending(packetSize);
            }
        }
        if (resp != null) {
            reqContext.writeRespInBizThreads(resp);
        }
    }
}
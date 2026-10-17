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
import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.Executor;
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
    private final int localPort;

    final int channelIndexInWorker;
    final long createTimeNanos;

    int seq = 1;

    private final MultiParser parser;

    // read status
    private ReadPacket packet;
    private boolean readBody;
    private PacketInfoReq requestForResp = null;
    private ReqProcessor processorForRequest;
    private int currentReadPacketSize;
    long packetStartTimeNanos;
    private DecoderCallback currentDecoderCallback;

    // linked list of channels with a packet under decoding, ordered by packetStartTimeNanos
    DtChannelImpl decodePrev, decodeNext;
    boolean inDecodeList;

    private boolean skipBody;

    // set while inside afterRead/processWriteEvent, a close request in that window is deferred
    boolean encodingOrDecoding;
    boolean closePending;
    Throwable closeCause;

    final IoChannelQueue subQueue;

    private boolean running = true;
    boolean closed;
    boolean handshake;
    boolean listenerOnConnectedCalled;

    PacketInfoReq pendingReqHead;
    PacketInfoReq pendingReqTail;

    public DtChannelImpl(NioStatus nioStatus, WorkerStatus workerStatus, NioConfig nioConfig, Peer peer,
                         SocketChannel socketChannel, int channelIndexInWorker, DecodeContext decodeContext) throws IOException {
        this.nioStatus = nioStatus;
        this.channel = socketChannel;
        this.nioConfig = nioConfig;
        this.workerStatus = workerStatus;
        this.channelIndexInWorker = channelIndexInWorker;
        this.peer = peer;
        this.createTimeNanos = workerStatus.ts.nanoTime;

        this.decodeContext = decodeContext;

        this.parser = new MultiParser(decodeContext, this, nioConfig.maxPacketSize);

        this.remoteAddr = channel.getRemoteAddress();
        this.localAddr = channel.getLocalAddress();
        this.localPort = ((java.net.InetSocketAddress) localAddr).getPort();

        this.subQueue = new IoChannelQueue(nioConfig, workerStatus, this);
    }

    public void afterRead(boolean running, ByteBuffer buf) {
        if (!running) {
            this.running = false;
        }
        encodingOrDecoding = true;
        try {
            parser.parse(buf);
        } finally {
            encodingOrDecoding = false;
        }
    }

    @Override
    public Object getResult() {
        return null;
    }

    @Override
    protected void begin(int len) {
        if (closed || closePending) {
            throw new NetException("channel closed");
        }
        this.currentReadPacketSize = len;
        this.packetStartTimeNanos = workerStatus.ts.nanoTime;
        workerStatus.addToDecodeList(this);
        packet = new ReadPacket();
        readBody = false;
        skipBody = false;
        requestForResp = null;
        processorForRequest = null;
        currentDecoderCallback = null;
    }

    @Override
    protected void end(boolean success) {
        if (!success) {
            cleanParseStatus(null, "unknown error");
            return;
        }
        if (skipBody) {
            cleanParseStatus(null, "response decoder is null");
            return;
        }

        if (!handshake && peer == null) { // peer is null is server side
            if (packet.command != Commands.CMD_HANDSHAKE || packet.packetType != PacketType.TYPE_REQ) {
                NetException e = new NetException("first command is not handshake, command=" + packet.command);
                cleanParseStatus(e, null);
                throw e;
            }
        }

        if (requestForResp == null && processorForRequest == null) {
            // empty body
            readBody(SimpleByteBufferPool.EMPTY_BUFFER, 0, 0);
            // readBody() may update skipBody
            if (skipBody) {
                cleanParseStatus(null, "response decoder is null");
                return;
            }
        }
        currentDecoderCallback = null;
        workerStatus.removeFromDecodeList(this);

        if (packet.packetType == PacketType.TYPE_RESP) {
            // assert requestForResp not null
            WritePacket req = requestForResp.packet;
            if (packet.command != req.command) {
                cleanParseStatus(null, "command not match");
                return;
            }
            ReadPacket p = packet;
            PacketInfoReq pir = requestForResp;
            packet = null; // not clean, ownership changed to biz code
            requestForResp = null;
            pir.callSuccess(p);
        } else {
            // req or one way
            ReadPacket p = packet;
            packet = null; // not clean, ownership changed to biz code
            processIncomingRequest(p, processorForRequest, workerStatus.ts);
        }
    }

    @Override
    public void readVarNumber(int index, long value) {
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
    }

    @Override
    public void readFix32(int index, int value) {
        if (this.readBody) {
            throw new PbException("body has read");
        }
        if (index == Packet.IDX_SEQ) {
            packet.seq = value;
        }
    }

    @Override
    public void readFix64(int index, long value) {
        if (this.readBody) {
            throw new PbException("body has read");
        }
        if (index == Packet.IDX_TIMEOUT) {
            packet.timeout = value;
        }
    }

    @Override
    public void readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (this.readBody) {
            throw new PbException("body has read");
        }
        switch (index) {
            case Packet.IDX_MSG: {
                this.packet.msg = parseUTF8(buf, fieldLen, currentPos);
                return;
            }
            case Packet.IDX_EXTRA: {
                this.packet.extra = parseBytes(buf, fieldLen, currentPos);
                return;
            }
            case Packet.IDX_BODY: {
                if (currentPos == 0 && currentDecoderCallback != null) {
                    throw new IllegalStateException("currentDecoder is not null");
                }
                boolean end = buf.remaining() >= fieldLen - currentPos;
                try {
                    readBody(buf, fieldLen, currentPos);
                } finally {
                    if (end) {
                        // so if the body is not last field, exception throws
                        readBody = true;
                    }
                }
                return;
            }
            default:
                return;
        }

    }

    private void readBody(ByteBuffer buf, int fieldLen, int currentPos) {
        if (skipBody) {
            buf.position(buf.limit());
            return;
        }
        if (packet.command <= 0) {
            throw new NetException("command invalid :" + packet.command);
        }
        // the body field should encode as last field
        if (!initRelatedDataForPacket()) {
            skipBody = true;
            buf.position(buf.limit());
            return;
        }
        if (currentDecoderCallback == null) {
            skipBody = true;
            buf.position(buf.limit());
            return;
        }
        ReadPacket p = packet;

        try {
            p.body = parseNested(buf, fieldLen, currentPos, currentDecoderCallback);
        } catch (RuntimeException | Error e) {
            cleanParseStatus(e, null); // update packet to null
            if (p.packetType == PacketType.TYPE_REQ) {
                int code = e instanceof CodecException ? CmdCodes.DECODE_ERROR : CmdCodes.SYS_ERROR;
                writeErrorInIoThread(p, code, null);
            }
            if (!handshake) {
                throw e;
            }
            skipBody = true;
            buf.position(buf.limit());
            log.warn("skip body parse, command={}", p.command, e);
        }
    }

    private boolean initRelatedDataForPacket() {
        ReadPacket packet = this.packet;
        if (packet.packetType == PacketType.TYPE_RESP) {
            PacketInfoReq requestForResp = this.requestForResp;
            if (requestForResp == null) {
                requestForResp = this.workerStatus.removePendingReq(channelIndexInWorker, packet.seq);
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
                        writeErrorInIoThread(packet, CmdCodes.COMMAND_NOT_SUPPORT,
                                "command " + packet.command + " not support");
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
        long delta = (bytes & 0x000000FF_FFFFFFFFL) + (1L << 40);
        nioStatus.getAndAddInPendingRelease(-delta);
    }

    private void processIncomingRequest(ReadPacket req, ReqProcessor p, Timestamp roundTime) {
        int maxReq = nioConfig.maxInRequests;
        long maxBytes = nioConfig.maxInBytes;

        Executor executor;
        if (p.useDefaultExecutor) {
            executor = workerStatus.worker.owner.bizExecutor;
        } else {
            executor = p.executor;
        }

        boolean flowControl;
        if (executor == null || (maxReq <= 0 && maxBytes <= 0)) {
            flowControl = false;
        } else {
            long delta = (currentReadPacketSize & 0x000000FF_FFFFFFFFL) + (1L << 40);
            long pending = nioStatus.getAndAddInPendingRelease(delta);
            int count = (int) ((pending & 0xFFFFFF00_00000000L) >>> 40);
            long bytes = pending & 0x000000FF_FFFFFFFFL;
            if (maxReq > 0 && count >= maxReq) {
                nioStatus.getAndAddInPendingRelease(-delta);
                log.debug("pendingRequests>maxInRequests({}), write response code FLOW_CONTROL to client",
                        nioConfig.maxInRequests);
                req.clean();
                writeErrorInIoThread(req, CmdCodes.FLOW_CONTROL,
                        "max incoming request: " + nioConfig.maxInRequests);
                return;
            }
            if (maxBytes > 0 && (bytes + currentReadPacketSize) > maxBytes) {
                nioStatus.getAndAddInPendingRelease(-delta);
                log.debug("pendingBytes({})>maxInBytes({}), write response code FLOW_CONTROL to client",
                        bytes, nioConfig.maxInBytes);
                req.clean();
                writeErrorInIoThread(req, CmdCodes.FLOW_CONTROL,
                        "max incoming request bytes: " + nioConfig.maxInBytes);
                return;
            }
            flowControl = true;
        }

        ReqContextImpl reqContext = new ReqContextImpl(this, req,
                new DtTime(roundTime, req.timeout, TimeUnit.NANOSECONDS), p, currentReadPacketSize, flowControl);

        if (executor == null) {
            WritePacket resp;
            try {
                if (timeout(req, reqContext, roundTime)) {
                    req.clean();
                    return;
                }
                // change res owner to biz code, not clean req by nio framework
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
            }
            if (resp != null) {
                resp.command = req.command;
                resp.packetType = PacketType.TYPE_RESP;
                resp.seq = req.seq;
                subQueue.enqueue(new PacketInfo(this, resp, reqContext.getTimeout()));
            }
        } else {
            try {
                executor.execute(reqContext);
            } catch (Throwable e) {
                req.clean();
                if (flowControl) {
                    releasePending(currentReadPacketSize);
                }
                if (e instanceof RejectedExecutionException) {
                    log.debug("catch RejectedExecutionException, write response code FLOW_CONTROL to client, maxInRequests={}",
                            nioConfig.maxInRequests);
                    writeErrorInIoThread(req, CmdCodes.FLOW_CONTROL,
                            "max incoming request: " + nioConfig.maxInRequests, reqContext.getTimeout());
                } else {
                    log.warn("executor execute fail, command={}", req.command, e);
                    writeErrorInIoThread(req, CmdCodes.SYS_ERROR, e.toString(), reqContext.getTimeout());
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
        subQueue.enqueue(new PacketInfo(this, resp, timeout));
    }

    public int getAndIncSeq() {
        if (seq < 0) {
            seq = 1;
        }
        return seq++;
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

    void closeOnDecodeTimeout(Timestamp ts) {
        if (closed) {
            return;
        }
        Packet p = packet;
        String msg = "packet decode timeout: " + TimeUnit.NANOSECONDS.toMillis(ts.nanoTime - packetStartTimeNanos)
                + "ms, cmd=" + (p == null ? 0 : p.command)
                + ", seq=" + (p == null ? 0 : p.seq) + ", remote=" + remoteAddr;
        log.warn("{}", msg);
        workerStatus.worker.close(this, new NetTimeoutException(msg));
    }

    // called only by NioWorker.doClose, which sets closed before this so reentrant close requests are rejected
    void close(Throwable ex) {
        try {
            if (ex != null && !(ex instanceof NetException)) {
                ex = new NetException("channel close", ex);
            }
            cleanParseStatus(ex, "channel close");
            decodeContext.reset(parser.getParser());
        } catch (Throwable e) {
            log.error("channel close error", e);
        }
    }

    private void cleanParseStatus(Throwable ex, String reason) {
        workerStatus.removeFromDecodeList(this);
        currentDecoderCallback = null;
        if (packet != null) {
            packet.clean();
            packet = null;
        }
        PacketInfoReq pir = requestForResp;
        if (pir != null) {
            requestForResp = null;
            pir.callFail(ex == null ? new NetException(reason) : ex);
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

    @Override
    public int getLocalPort() {
        return localPort;
    }
}

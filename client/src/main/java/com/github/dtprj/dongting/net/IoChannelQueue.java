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

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.common.DtBugException;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
class IoChannelQueue {
    private static final DtLog log = DtLogs.getLogger(IoChannelQueue.class);

    private static final int ENCODE_NOT_FINISH = 1;
    private static final int ENCODE_FINISH = 2;
    private static final int ENCODE_CANCEL = 3;

    private static final int MAX_BUFFER_SIZE = 256 * 1024;
    private final WorkerStatus workerStatus;
    private final DtChannelImpl dtc;
    private SelectionKey selectionKey;

    private ByteBuffer writeBuffer;
    private final ArrayList<ByteBuffer> writeBufList = new ArrayList<>();
    private ByteBuffer[] gatheringWriteArray;
    private int bytesToWrite;
    private int packetsInBuffer;

    private final ArrayList<WritePacket> pendingCleanPackets = new ArrayList<>();

    private final IndexedQueue<PacketInfo> subQueue = new IndexedQueue<>(8);
    private PacketInfoReq oneWayCallback;
    private IndexedQueue<PacketInfoReq> oneWayCallbacks;
    private boolean writing;

    private PacketInfo lastPacketInfo;
    private final EncodeContext encodeContext;

    private final PerfCallback perfCallback;

    public IoChannelQueue(NioConfig config, WorkerStatus workerStatus, DtChannelImpl dtc) {
        this.workerStatus = workerStatus;
        this.dtc = dtc;
        this.encodeContext = new EncodeContext(workerStatus.buffers);
        this.perfCallback = config.perfCallback;
    }

    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    private void callFail(PacketInfo pi, boolean callClean, Throwable ex) {
        if (callClean) {
            pi.packet.clean();
        }
        if (pi instanceof PacketInfoReq) {
            PacketInfoReq req = (PacketInfoReq) pi;
            req.callFail(ex);
        }
    }

    public void enqueue(PacketInfo packetInfo) {
        WritePacket wf = packetInfo.packet;
        if (wf.use) {
            callFail(packetInfo, false, new DtBugException("WritePacket is used"));
            return;
        }
        wf.use = true;

        packetInfo.perfTimeOrAddOrder = perfCallback.takeTimeAndRefresh(PerfConsts.RPC_D_CHANNEL_QUEUE, workerStatus.ts);
        subQueue.addLast(packetInfo);

        if (subQueue.size() == 1 && !writing) {
            selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            perfCallback.fire(PerfConsts.RPC_C_MARK_WRITE);
        }
        workerStatus.addPacketsToWrite(1);
    }

    public void cleanChannelQueue() {
        this.writeBufList.clear();
        this.bytesToWrite = 0;
        cleanPendingPackets();
        if (packetsInBuffer > 0) {
            workerStatus.addPacketsToWrite(-packetsInBuffer);
            packetsInBuffer = 0;
        }
        if (this.writeBuffer != null) {
            workerStatus.buffers.release(this.writeBuffer);
            this.writeBuffer = null;
        }

        if (lastPacketInfo != null) {
            workerStatus.addPacketsToWrite(-1);
            callFail(lastPacketInfo, true, new NetException("channel closed, cancel request still in IoChannelQueue. 1"));
        }
        PacketInfo pi;
        while ((pi = subQueue.pollFirst()) != null) {
            callFail(pi, true, new NetException("channel closed, cancel request still in IoChannelQueue. 2"));
            workerStatus.addPacketsToWrite(-1);
        }
        cleanOneWayCallbacks(new NetException("channel closed, cancel oneway request still in IoChannelQueue."));
    }

    private void cleanPendingPackets() {
        if (!pendingCleanPackets.isEmpty()) {
            for (WritePacket wp : pendingCleanPackets) {
                wp.clean();
            }
            pendingCleanPackets.clear();
        }
    }

    private void afterWrite(int bytes) {
        bytesToWrite -= bytes;
        if (bytesToWrite < 0) {
            BugLog.log("bytesToWrite is negative: {}", bytesToWrite);
            bytesToWrite = 0;
        }
        if (bytesToWrite > 0) {
            return;
        }

        // all data fully written
        if (!writeBufList.isEmpty()) {
            for (int size = writeBufList.size(), i = 0; i < size; i++) {
                gatheringWriteArray[i] = null;
            }
            writeBufList.clear();
        }
        cleanPendingPackets();
        workerStatus.addPacketsToWrite(-packetsInBuffer);
        workerStatus.buffers.release(writeBuffer);
        this.writeBuffer = null;
        packetsInBuffer = 0;
        cleanOneWayCallbacks(null);
    }

    private void cleanOneWayCallbacks(Throwable ex) {
        if (oneWayCallback != null) {
            finishOneWayCallback(oneWayCallback, ex);
            oneWayCallback = null;
        }
        if (oneWayCallbacks != null) {
            PacketInfoReq pi;
            while ((pi = oneWayCallbacks.pollFirst()) != null) {
                finishOneWayCallback(pi, ex);
            }
        }
    }

    private void finishOneWayCallback(PacketInfoReq pi, Throwable ex) {
        if (ex == null) {
            pi.callSuccess(null);
        } else {
            callFail(pi, false, ex);
        }
    }

    public boolean prepareWriteBuffer(Timestamp roundTime) {
        // writeBufList still has unwritten segments
        if (!writeBufList.isEmpty()) {
            return true;
        }

        if (writeBuffer != null) {
            if (writeBuffer.remaining() > 0) {
                return true;
            } else {
                BugLog.log("writeBuffer is not null but remaining is 0");
                workerStatus.buffers.release(writeBuffer);
                this.writeBuffer = null;
            }
        }

        IndexedQueue<PacketInfo> subQueue = this.subQueue;
        if (subQueue.size() == 0 && lastPacketInfo == null) {
            // no packet to write
            return false;
        }

        ByteBuffer buf = alloc(roundTime);

        try {
            encodePacketsToBuffer(buf, subQueue, roundTime);
        } catch (RuntimeException | Error e) {
            encodeContext.reset();
            // channel will be closed, and cleanChannelQueue will be called
            workerStatus.buffers.release(buf);
            throw e;
        }
        buf.flip();

        if (writeBufList.isEmpty()) {
            if (buf.remaining() == 0) {
                workerStatus.buffers.release(buf);
                return false;
            }
            this.writeBuffer = buf;
            bytesToWrite = buf.remaining();
            return true;
        } else {
            this.writeBuffer = buf;
            bytesToWrite = 0;
            for (int size = writeBufList.size(), i = 0; i < size; i++) {
                bytesToWrite += writeBufList.get(i).remaining();
            }
            return true;
        }
    }

    private ByteBuffer alloc(Timestamp roundTime) {
        // not accurate
        // can't invoke actualSize() here because seq and timeout field is not set yet
        int totalSize = 0;
        if (lastPacketInfo != null) {
            WritePacket lastPacket = lastPacketInfo.packet;
            int rest = lastPacket.calcMaxPacketSize() - lastPacketInfo.encodedBytes;
            if (rest <= 0) {
                BugLog.log("rest is {}, packetClass={}", rest, lastPacket.getClass().getName());
                return workerStatus.buffers.borrowDirect(128);
            }
            totalSize += rest;
            if (totalSize > MAX_BUFFER_SIZE) {
                return workerStatus.buffers.borrowDirect(MAX_BUFFER_SIZE);
            }
        }
        for (int size = subQueue.size(), i = 0; i < size; i++) {
            PacketInfo pi = subQueue.get(i);
            if (pi.timeout.deadlineNanos - roundTime.nanoTime <= 0) {//keep same with encode method
                continue;
            }
            if (pi.packet.isPreEncoded()) {
                // use calcMaxPacketSize() instead of actualSize(), because seq/timeout are not set yet
                // only header is written to writeBuffer, body goes to writeBufList via gathering write
                totalSize += pi.packet.calcMaxPacketSize() - pi.packet.actualBodySize();
                if (totalSize >= MAX_BUFFER_SIZE) {
                    break;
                }
            } else {
                totalSize += pi.packet.calcMaxPacketSize();
                // only normal packet may be "truncated"
                if (totalSize >= MAX_BUFFER_SIZE) {
                    return workerStatus.buffers.borrowDirect(MAX_BUFFER_SIZE);
                }
            }
        }
        if (totalSize <= 0) {
            return workerStatus.buffers.borrowDirect(128);
        }
        return workerStatus.buffers.borrowDirect(totalSize);
    }

    private void encodePacketsToBuffer(ByteBuffer buf, IndexedQueue<PacketInfo> subQueue, Timestamp roundTime) {
        int lastSlicePos = 0;
        writeBufList.clear();
        PacketInfo pi = this.lastPacketInfo;
        try {
            while (subQueue.size() > 0 || pi != null) {
                int encodeResult;
                int oldPos = buf.position();
                if (pi == null) {
                    pi = subQueue.pollFirst();
                    perfCallback.fireTimeAndRefresh(PerfConsts.RPC_D_CHANNEL_QUEUE, pi.perfTimeOrAddOrder, 1, 0, workerStatus.ts);
                    encodeResult = encode(buf, pi, roundTime);
                } else {
                    encodeResult = doEncode(buf, pi);
                }
                pi.encodedBytes += buf.position() - oldPos;
                if (encodeResult == ENCODE_NOT_FINISH) {
                    if (buf.position() == 0) {
                        workerStatus.addPacketsToWrite(-1);
                        encodeContext.reset();
                        NetException ex = new NetException("encode fail when buffer is empty");
                        BugLog.log(ex);
                        callFail(pi, true, ex);
                        pi = null;
                        throw ex;
                    }
                    // ensure writeBufList covers all encoded data in writeBuffer
                    if (!writeBufList.isEmpty()) {
                        sliceBuf(buf, lastSlicePos, writeBufList);
                    }
                    return;
                }
                boolean isPreEncoded = false;
                ByteBuffer preEncodedBuf = null;
                try {
                    if (encodeResult == ENCODE_FINISH) {
                        handleEncodeFinish(pi);
                        isPreEncoded = pi.packet.isPreEncoded();
                        if (isPreEncoded) {
                            preEncodedBuf = pi.packet.getPreEncodedBuffer();
                            pendingCleanPackets.add(pi.packet);
                        } else {
                            pi.packet.clean();
                        }
                    } else {
                        handleEncodeCancel(pi);
                        pi.packet.clean();
                    }
                } finally {
                    encodeContext.reset();
                    pi = null;
                }
                if (isPreEncoded) {
                    // slice writeBuffer data before the preEncoded body
                    sliceBuf(buf, lastSlicePos, writeBufList);
                    // add preEncoded body buffer
                    if (preEncodedBuf != null) {
                        writeBufList.add(preEncodedBuf);
                    }
                    lastSlicePos = buf.position();
                }
            }
        } finally {
            this.lastPacketInfo = pi;
        }
        // slice the last segment if there was any preEncoded packet
        if (lastSlicePos > 0) {
            sliceBuf(buf, lastSlicePos, writeBufList);
        }
    }

    private static void sliceBuf(ByteBuffer buf, int from, ArrayList<ByteBuffer> list) {
        int pos = buf.position();
        if (from >= pos) {
            return;
        }
        int oldLimit = buf.limit();
        buf.position(from);
        buf.limit(pos);
        list.add(buf.slice());
        buf.limit(oldLimit);
        buf.position(pos);
    }

    private void handleEncodeFinish(PacketInfo pi) {
        WritePacket f = pi.packet;
        if (f.packetType == PacketType.TYPE_REQ) {
            workerStatus.addPendingReq((PacketInfoReq) pi);
        }
        packetsInBuffer++;
        if (f.packetType == PacketType.TYPE_ONE_WAY) {
            // TYPE_ONE_WAY is always PacketInfoReq, see NioNet.send0()
            if (oneWayCallback == null) {
                oneWayCallback = (PacketInfoReq) pi;
            } else {
                if (oneWayCallbacks == null) {
                    oneWayCallbacks = new IndexedQueue<>(4);
                }
                oneWayCallbacks.addLast((PacketInfoReq) pi);
            }
        }
    }

    private void handleEncodeCancel(PacketInfo pi) {
        workerStatus.addPacketsToWrite(-1);
        String msg = "timeout before send: " + pi.timeout.getTimeout(TimeUnit.MILLISECONDS) + "ms";
        callFail(pi, false, new NetTimeoutException(msg));
    }

    private int encode(ByteBuffer buf, PacketInfo pi, Timestamp roundTime) {
        WritePacket f = pi.packet;
        // request or one way request
        boolean request = f.packetType != PacketType.TYPE_RESP;
        DtTime t = pi.timeout;
        long rest = t.deadlineNanos - roundTime.nanoTime;
        if (rest <= 0) {
            if (request) {
                log.warn("request timeout before send: {}ms, cmd={}, seq={}, channel={}",
                        t.getTimeout(TimeUnit.MILLISECONDS), f.command, f.seq, pi.dtc.getChannel());
            } else {
                log.warn("response timeout before send: {}ms, cmd={}, seq={}, channel={}",
                        t.getTimeout(TimeUnit.MILLISECONDS), f.command, f.seq, pi.dtc.getChannel());
            }
            return ENCODE_CANCEL;
        }

        if (request) {
            f.seq = dtc.getAndIncSeq();
            f.timeout = rest;
        }
        encodeContext.reset();
        return doEncode(buf, pi);
    }

    private int doEncode(ByteBuffer buf, PacketInfo pi) {
        WritePacket wf = pi.packet;
        if (wf.isPreEncoded()) {
            wf.writeHeader(buf);
            return ENCODE_FINISH;
        }
        return wf.encode(encodeContext, buf) ? ENCODE_FINISH : ENCODE_NOT_FINISH;
    }

    public void processWriteEvent(SocketChannel sc, SelectionKey key, Timestamp roundTime) throws IOException {
        if (prepareWriteBuffer(roundTime)) {
            writing = true;
            long startTime = perfCallback.takeTimeAndRefresh(PerfConsts.RPC_D_WRITE, roundTime);
            int bytes;
            if (!writeBufList.isEmpty()) {
                int listSize = writeBufList.size();
                if (gatheringWriteArray == null || gatheringWriteArray.length < listSize) {
                    gatheringWriteArray = new ByteBuffer[Math.max(listSize, 8)];
                }
                writeBufList.toArray(gatheringWriteArray);
                bytes = (int) sc.write(gatheringWriteArray, 0, listSize);
            } else {
                bytes = sc.write(writeBuffer);
            }
            perfCallback.fireTimeAndRefresh(PerfConsts.RPC_D_WRITE, startTime, 1, bytes, roundTime);
            afterWrite(bytes);
        } else {
            // no data to write
            writing = false;
            key.interestOps(SelectionKey.OP_READ);
            perfCallback.fire(PerfConsts.RPC_C_MARK_READ);
        }
    }
}

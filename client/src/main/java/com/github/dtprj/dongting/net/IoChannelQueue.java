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
    private static final int MAX_GATHER_SIZE = 64;
    private final WorkerStatus workerStatus;
    private final DtChannelImpl dtc;
    private SelectionKey selectionKey;

    private ByteBuffer writeBuffer;
    private final ArrayList<ByteBuffer> writeBufList = new ArrayList<>();
    private ByteBuffer[] gatheringBufferCache;
    private int gatheringBufferCacheOffset = -1;
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
        if (pi instanceof PacketInfoReq) {
            PacketInfoReq req = (PacketInfoReq) pi;
            req.callFail(ex);
        }
        if (callClean) {
            pi.packet.clean();
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

    // called when channel is closing
    public void cleanChannelQueue() {
        this.writeBufList.clear();
        this.gatheringBufferCacheOffset = -1;
        this.bytesToWrite = 0;
        cleanPendingPackets();
        if (packetsInBuffer > 0) {
            workerStatus.addPacketsToWrite(-packetsInBuffer);
            packetsInBuffer = 0;
        }
        releaseWriteBuffer();

        if (lastPacketInfo != null) {
            workerStatus.addPacketsToWrite(-1);
            callFail(lastPacketInfo, true, new NetException("channel closed, cancel request still in IoChannelQueue. 1"));
            lastPacketInfo = null;
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
        // clean is called
        // normal packet: after encode finish in encodePacketsToBuffer()
        // pre-encoded packet: in cleanPendingPackets(), called in cleanChannelQueue()/processWriteEvent()
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
                releaseWriteBuffer();
            }
        }

        IndexedQueue<PacketInfo> subQueue = this.subQueue;
        if (subQueue.size() == 0 && lastPacketInfo == null) {
            // no packet to write
            return false;
        }

        ByteBuffer buf = alloc(roundTime);
        this.writeBuffer = buf;

        try {
            encodePacketsToBuffer(buf, subQueue, roundTime);
        } catch (RuntimeException | Error e) {
            encodeContext.reset();
            // channel will be closed, and cleanChannelQueue will be called
            releaseWriteBuffer();
            throw e;
        }
        buf.flip();

        if (writeBufList.isEmpty()) {
            if (buf.remaining() == 0) {
                releaseWriteBuffer();
                return false;
            }
            bytesToWrite = buf.remaining();
        } else {
            bytesToWrite = 0;
            for (int size = writeBufList.size(), i = 0; i < size; i++) {
                bytesToWrite += writeBufList.get(i).remaining();
            }
        }
        return true;
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
                return SimpleByteBufferPool.EMPTY_BUFFER;
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
            int packetSize = pi.packet.calcMaxPacketSize() - pi.packet.getTotalPreEncodedBufferSize();
            totalSize += packetSize;
            if (totalSize >= MAX_BUFFER_SIZE) {
                return workerStatus.buffers.borrowDirect(MAX_BUFFER_SIZE);
            }
        }
        if (totalSize <= 0) {
            // all packet timeout, not bug
            log.info("total size is {}", totalSize);
            return SimpleByteBufferPool.EMPTY_BUFFER;
        }
        return workerStatus.buffers.borrowDirect(totalSize);
    }

    private void encodePacketsToBuffer(ByteBuffer buf, IndexedQueue<PacketInfo> subQueue, Timestamp roundTime) {
        int lastSlicePos = 0;
        writeBufList.clear();
        PacketInfo pi = this.lastPacketInfo;
        try {
            while (pi != null || subQueue.size() > 0) {
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
                    if (pi.packet.hasPreEncodedBuffer()) {
                        // encode() stopped because next item is a preEncoded buffer
                        lastSlicePos = sliceBuf(buf, lastSlicePos, writeBufList);
                        ByteBuffer preBuf = pi.packet.getPreEncodedBuffer();
                        if (preBuf == null || preBuf.remaining() == 0) {
                            PacketInfo oldPi = pi;
                            pi = null;
                            throw createBugEx(oldPi, "preEncoded buffer is null or empty");
                        }
                        writeBufList.add(preBuf);

                        // see WritePacket.encode(), it will check pending field after encode finish
                        encodeContext.pending += preBuf.remaining();

                        pi.encodedBytes += preBuf.remaining();
                        // pi stays set, loop continues to re-encode same packet
                        continue;
                    }
                    if (buf.position() == 0) {
                        PacketInfo oldPi = pi;
                        pi = null;
                        throw createBugEx(oldPi, "encode fail when buffer is empty");
                    }
                    if (!writeBufList.isEmpty()) {
                        sliceBuf(buf, lastSlicePos, writeBufList);
                    }
                    return;
                }
                try {
                    if (encodeResult == ENCODE_FINISH) {
                        handleEncodeFinish(pi);
                        if (pi.packet.hasPreEncodedBuffer()) {
                            DtBugException ex = new DtBugException("hasPreEncodedBuffer should be false after encode finished");
                            callFail(pi, true, ex);
                            BugLog.log(ex);
                            throw ex;
                        }
                        if (pi.packet.getTotalPreEncodedBufferSize() <= 0) {
                            pi.packet.clean();
                        } else {
                            pendingCleanPackets.add(pi.packet);
                        }
                        if (!writeBufList.isEmpty()) {
                            lastSlicePos = sliceBuf(buf, lastSlicePos, writeBufList);
                        }
                    } else {
                        handleEncodeCancel(pi);
                    }
                } finally {
                    encodeContext.reset();
                    pi = null;
                }
            }
        } finally {
            this.lastPacketInfo = pi;
        }
    }

    private DtBugException createBugEx(PacketInfo pi, String msg) {
        workerStatus.addPacketsToWrite(-1);
        encodeContext.reset();
        DtBugException ex = new DtBugException(msg);
        BugLog.log(ex);
        callFail(pi, true, ex);
        return ex;
    }

    private static int sliceBuf(ByteBuffer buf, int from, ArrayList<ByteBuffer> list) {
        int pos = buf.position();
        if (from >= pos) {
            return from;
        }
        int oldLimit = buf.limit();
        buf.position(from);
        buf.limit(pos);
        list.add(buf.slice());
        buf.limit(oldLimit);
        buf.position(pos);
        return pos;
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
        callFail(pi, true, new NetTimeoutException(msg));
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
        return pi.packet.encode(encodeContext, buf) ? ENCODE_FINISH : ENCODE_NOT_FINISH;
    }

    public void processWriteEvent(SocketChannel sc, SelectionKey key, Timestamp roundTime) throws IOException {
        if (prepareWriteBuffer(roundTime)) {
            writing = true;
            long startTime = perfCallback.takeTimeAndRefresh(PerfConsts.RPC_D_WRITE, roundTime);
            int bytes;
            if (!writeBufList.isEmpty()) {
                int listSize = writeBufList.size();
                int offset = gatheringBufferCacheOffset;
                ByteBuffer[] arr = this.gatheringBufferCache;
                if (offset < 0) {
                    if (arr == null || arr.length < listSize) {
                        arr = new ByteBuffer[Math.max(listSize, 8)];
                        this.gatheringBufferCache = arr;
                    }
                    writeBufList.toArray(arr);
                    offset = 0;
                }
                bytes = 0;
                WRITE_LOOP:
                while (offset < listSize) {
                    // split into multiple calls because gathering write may has limit (jdk11 not split internally)
                    int batchSize = Math.min(MAX_GATHER_SIZE, listSize - offset);
                    int written = (int) sc.write(arr, offset, batchSize);
                    bytes += written;
                    for (int i = offset, limit = offset + batchSize; i < limit; i++) {
                        if (arr[i].remaining() > 0) {
                            offset = i;
                            break WRITE_LOOP;
                        }
                    }
                    offset += batchSize;
                }
                gatheringBufferCacheOffset = offset;
            } else {
                bytes = sc.write(writeBuffer);
            }
            perfCallback.fireTimeAndRefresh(PerfConsts.RPC_D_WRITE, startTime, 1, bytes, roundTime);

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
                    gatheringBufferCache[i] = null;
                }
                writeBufList.clear();
                gatheringBufferCacheOffset = -1;
            }
            cleanPendingPackets();
            workerStatus.addPacketsToWrite(-packetsInBuffer);
            releaseWriteBuffer();
            packetsInBuffer = 0;
            cleanOneWayCallbacks(null);
        } else {
            // no data to write
            writing = false;
            key.interestOps(SelectionKey.OP_READ);
            perfCallback.fire(PerfConsts.RPC_C_MARK_READ);
        }
    }

    private void releaseWriteBuffer() {
        ByteBuffer bb = this.writeBuffer;
        if (bb != null) {
            if (bb.capacity() > 0) {
                workerStatus.buffers.release(bb);
            }
            writeBuffer = null;
        }
    }
}

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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.raft.rpc.PreEncodedWritePacket;

import java.nio.ByteBuffer;
import java.util.List;

/** Owns the response buffer and pinned TailCache buffers until clean(). */
public final class MqPullResp extends PreEncodedWritePacket {
    private static final int IDX_RESULTS_SIZE = 1;
    private static final int IDX_RESULTS = 2;
    private static final int IDX_RECORDS = 15;

    // MqPullRespDetail
    private static final int IDX_DETAIL_QUEUE_ID = 1;
    private static final int IDX_DETAIL_FROM_SEQ = 2;
    private static final int IDX_DETAIL_COUNT = 3;
    private static final int IDX_DETAIL_TRUNCATED = 4;

    private final RefBuffer buffer;
    private final RefBuffer[] cached;
    private final List<ByteBuffer> segments;
    private int nextSegment;

    MqPullResp(RefBuffer buffer, RefBuffer[] cached, List<ByteBuffer> segments, int bodySize) {
        this.buffer = buffer;
        this.cached = cached;
        this.segments = segments;
        this.totalPreEncodedSize = bodySize;
    }

    static int headerSize(long[] queueIds, long[] fromSeqs, MqPullFrame.QueueResult[] results, int bytes) {
        int size = PbUtil.sizeOfInt32Field(IDX_RESULTS_SIZE, queueIds.length);
        for (int i = 0; i < queueIds.length; i++) {
            int n = resultSize(queueIds[i], fromSeqs[i], results == null ? null : results[i]);
            size = Math.addExact(size, PbUtil.sizeOfLenFieldPrefix(IDX_RESULTS, n) + n);
        }
        return Math.addExact(size, bytes == 0 ? 0 : PbUtil.sizeOfLenFieldPrefix(IDX_RECORDS, bytes));
    }

    private static int resultSize(long queueId, long requestedSeq, MqPullFrame.QueueResult r) {
        return PbUtil.sizeOfFix64Field(IDX_DETAIL_QUEUE_ID, queueId)
                + PbUtil.sizeOfFix64Field(IDX_DETAIL_FROM_SEQ, r == null ? requestedSeq : r.fromSeq)
                + PbUtil.sizeOfInt32Field(IDX_DETAIL_COUNT, r == null ? 0 : r.queueCount)
                + PbUtil.sizeOfInt32Field(IDX_DETAIL_TRUNCATED, r != null && r.truncated ? 1 : 0);
    }

    static void writeHeader(ByteBuffer dest, long[] queueIds, long[] fromSeqs,
                            MqPullFrame.QueueResult[] results, int bytes) {
        PbUtil.writeInt32Field(dest, IDX_RESULTS_SIZE, queueIds.length);
        for (int i = 0; i < queueIds.length; i++) {
            MqPullFrame.QueueResult r = results == null ? null : results[i];
            PbUtil.writeLenFieldPrefix(dest, IDX_RESULTS, resultSize(queueIds[i], fromSeqs[i], r));
            PbUtil.writeFix64Field(dest, IDX_DETAIL_QUEUE_ID, queueIds[i]);
            PbUtil.writeFix64Field(dest, IDX_DETAIL_FROM_SEQ, r == null ? fromSeqs[i] : r.fromSeq);
            PbUtil.writeInt32Field(dest, IDX_DETAIL_COUNT, r == null ? 0 : r.queueCount);
            PbUtil.writeInt32Field(dest, IDX_DETAIL_TRUNCATED, r != null && r.truncated ? 1 : 0);
        }
        if (bytes > 0) {
            PbUtil.writeLenFieldPrefix(dest, IDX_RECORDS, bytes);
        }
    }

    @Override
    protected int calcActualBodySize() {
        return totalPreEncodedSize;
    }

    @Override
    protected boolean encodeBody(EncodeContext context, ByteBuffer dest) {
        if (segments == null) {
            if (nextSegment != 0) {
                return true;
            }
            nextSegment = 1;
            preEncodedBuffer = buffer.getBuffer();
        } else {
            if (nextSegment == segments.size()) {
                return true;
            }
            preEncodedBuffer = segments.get(nextSegment++);
        }
        return false;
    }

    @Override
    protected void doClean() {
        buffer.release();
        if (cached != null) {
            for (RefBuffer ref : cached) {
                if (ref != null) {
                    ref.release();
                }
            }
        }
    }
}

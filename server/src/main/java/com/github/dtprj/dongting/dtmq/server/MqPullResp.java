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

    static int headerSize(MqPullFrame.QueueResult[] results, int bytes) {
        int size = PbUtil.sizeOfInt32Field(1, results.length);
        for (MqPullFrame.QueueResult r : results) {
            int n = resultSize(r);
            size = Math.addExact(size, PbUtil.sizeOfLenFieldPrefix(2, n) + n);
        }
        return Math.addExact(size, bytes == 0 ? 0 : PbUtil.sizeOfLenFieldPrefix(15, bytes));
    }

    private static int resultSize(MqPullFrame.QueueResult r) {
        return PbUtil.sizeOfFix64Field(1, r.queueId)
                + PbUtil.sizeOfFix64Field(2, r.fromSeq)
                + PbUtil.sizeOfFix64Field(3, r.nextSeq)
                + PbUtil.sizeOfInt32Field(4, r.count)
                + PbUtil.sizeOfInt32Field(5, r.truncated ? 1 : 0)
                + PbUtil.sizeOfInt32Field(6, r.requiredBytes);
    }

    static void writeHeader(ByteBuffer dest, MqPullFrame.QueueResult[] results, int bytes) {
        PbUtil.writeInt32Field(dest, 1, results.length);
        for (MqPullFrame.QueueResult r : results) {
            PbUtil.writeLenFieldPrefix(dest, 2, resultSize(r));
            PbUtil.writeFix64Field(dest, 1, r.queueId);
            PbUtil.writeFix64Field(dest, 2, r.fromSeq);
            PbUtil.writeFix64Field(dest, 3, r.nextSeq);
            PbUtil.writeInt32Field(dest, 4, r.count);
            PbUtil.writeInt32Field(dest, 5, r.truncated ? 1 : 0);
            PbUtil.writeInt32Field(dest, 6, r.requiredBytes);
        }
        if (bytes > 0) {
            PbUtil.writeLenFieldPrefix(dest, 15, bytes);
        }
    }

    @Override
    protected int calcActualBodySize() {
        return totalPreEncodedSize;
    }

    @Override
    protected boolean encodeBody(EncodeContext context, ByteBuffer dest) {
        if (nextSegment == segments.size()) {
            return true;
        }
        preEncodedBuffer = segments.get(nextSegment++);
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

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
package com.github.dtprj.dongting.dtmq.codec;

import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbUtil;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class MqPullReq implements Encodable {
    public static final int IDX_GROUP_ID = 1;
    public static final int IDX_QUEUES_SIZE = 2;
    public static final int IDX_LIMIT = 3;
    public static final int IDX_BYTES_LIMIT = 4;
    public static final int IDX_WAIT_MILLIS = 5;
    public static final int IDX_QUEUES = 15;

    // MqPullReqDetail
    public static final int IDX_DETAIL_QUEUE_ID = 1;
    public static final int IDX_DETAIL_FROM_SEQ = 2;

    public int groupId;
    // 1-based queue ids; 0 is reserved
    public long[] queueIds;
    // 0 starts at the earliest readable record
    public long[] fromSeqs;
    // shared record limit hint across all queues
    public int limit;
    // shared raw log byte hint (no metadata); any single record may cross it, collection stops once reached
    public int bytesLimit;
    // 0 disables long polling
    public long waitMillis;

    private int encodeSize;
    private final DetailEncoder detailEncoder = new DetailEncoder();

    public MqPullReq() {
    }

    public MqPullReq(int groupId, long[] queueIds, long[] fromSeqs, int limit, int bytesLimit, long waitMillis) {
        this.groupId = groupId;
        this.queueIds = queueIds;
        this.fromSeqs = fromSeqs;
        this.limit = limit;
        this.bytesLimit = bytesLimit;
        this.waitMillis = waitMillis;
    }

    private void checkQueues() {
        int idLen = queueIds == null ? 0 : queueIds.length;
        int seqLen = fromSeqs == null ? 0 : fromSeqs.length;
        if (idLen != seqLen) {
            throw new IllegalArgumentException("queueIds and fromSeqs length mismatch");
        }
    }

    @Override
    public int actualSize() {
        if (encodeSize == 0) {
            checkQueues();
            int count = queueIds == null ? 0 : queueIds.length;
            int size = PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupId)
                    + PbUtil.sizeOfInt32Field(IDX_QUEUES_SIZE, count)
                    + PbUtil.sizeOfInt32Field(IDX_LIMIT, limit)
                    + PbUtil.sizeOfInt32Field(IDX_BYTES_LIMIT, bytesLimit)
                    + PbUtil.sizeOfInt64Field(IDX_WAIT_MILLIS, waitMillis);
            for (int i = 0; i < count; i++) {
                int detailSize = detailSize(queueIds[i], fromSeqs[i]);
                size += PbUtil.sizeOfLenFieldPrefix(IDX_QUEUES, detailSize) + detailSize;
            }
            encodeSize = size;
        }
        return encodeSize;
    }

    private static int detailSize(long queueId, long fromSeq) {
        return PbUtil.sizeOfFix64Field(IDX_DETAIL_QUEUE_ID, queueId)
                + PbUtil.sizeOfFix64Field(IDX_DETAIL_FROM_SEQ, fromSeq);
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_GROUP_ID, groupId)) {
                    return false;
                }
                // fall through
            case IDX_GROUP_ID:
                int count = queueIds == null ? 0 : queueIds.length;
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_QUEUES_SIZE, count)) {
                    return false;
                }
                // fall through
            case IDX_QUEUES_SIZE:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_LIMIT, limit)) {
                    return false;
                }
                // fall through
            case IDX_LIMIT:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_BYTES_LIMIT, bytesLimit)) {
                    return false;
                }
                // fall through
            case IDX_BYTES_LIMIT:
                if (!EncodeUtil.encodeInt64(context, destBuffer, IDX_WAIT_MILLIS, waitMillis)) {
                    return false;
                }
                // fall through
            case IDX_WAIT_MILLIS:
                return encodeQueues(context, destBuffer);
            default:
                throw new CodecException(context);
        }
    }

    private boolean encodeQueues(EncodeContext context, ByteBuffer destBuffer) {
        int count = queueIds == null ? 0 : queueIds.length;
        if (count == 0) {
            context.stage = IDX_QUEUES;
            return true;
        }
        // the nested stage is the current queue index; the detail encoder holds the rest of the state
        EncodeContext qc = context.createOrGetNestedContext(false);
        for (int index = qc.stage; index < count; index++) {
            detailEncoder.queueId = queueIds[index];
            detailEncoder.fromSeq = fromSeqs[index];
            if (!EncodeUtil.encode(qc, destBuffer, IDX_QUEUES, detailEncoder)) {
                return false;
            }
            qc.stage = index + 1;
        }
        context.stage = IDX_QUEUES;
        return true;
    }

    private static final class DetailEncoder implements Encodable {
        private long queueId;
        private long fromSeq;

        @Override
        public int actualSize() {
            return detailSize(queueId, fromSeq);
        }

        @Override
        public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
            switch (context.stage) {
                case EncodeContext.STAGE_BEGIN:
                    if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_DETAIL_QUEUE_ID, queueId)) {
                        return false;
                    }
                    // fall through
                case IDX_DETAIL_QUEUE_ID:
                    return EncodeUtil.encodeFix64(context, destBuffer, IDX_DETAIL_FROM_SEQ, fromSeq);
                default:
                    throw new CodecException(context);
            }
        }
    }
}

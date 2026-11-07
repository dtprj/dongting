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

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.ParseUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.dtmq.MqPullReq;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author huangli
 */
public final class MqPullReqCallback extends MqPullReq implements PbCallback<MqPullReq> {
    private static final int MIN_CAPACITY = 16;

    private final QueueCallback queueCallback = new QueueCallback();
    // -1 means the request doesn't declare queues_size
    private int declaredSize = -1;
    private int queueCount;

    @Override
    public void readVarNumber(DecodeContext context, int index, long value) {
        switch (index) {
            case MqPullReq.IDX_GROUP_ID:
                groupId = (int) value;
                break;
            case MqPullReq.IDX_QUEUES_SIZE:
                if (value < 0 || value > Integer.MAX_VALUE) {
                    throw new PbException("invalid queues_size: " + value);
                }
                declaredSize = (int) value;
                break;
            case MqPullReq.IDX_LIMIT:
                limit = (int) value;
                break;
            case MqPullReq.IDX_BYTES_LIMIT:
                bytesLimit = (int) value;
                break;
            case MqPullReq.IDX_WAIT_MILLIS:
                waitMillis = value;
                break;
        }
    }

    @Override
    public void readBytes(DecodeContext context, int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index != MqPullReq.IDX_QUEUES) {
            return;
        }
        if (currentPos == 0) {
            if (queueIds == null) {
                int capacity = initialCapacity();
                queueIds = new long[capacity];
                fromSeqs = new long[capacity];
            } else if (queueCount == queueIds.length) {
                int capacity = (int) Math.min((long) queueCount * 2, Integer.MAX_VALUE - 8);
                queueIds = Arrays.copyOf(queueIds, capacity);
                fromSeqs = Arrays.copyOf(fromSeqs, capacity);
            }
        }
        if (ParseUtil.parseNested(context, buf, fieldLen, currentPos, queueCallback) != null) {
            queueCount++;
        }
    }

    private int initialCapacity() {
        int capacity = ParseUtil.checkSuggestedSize(declaredSize > 0 ? declaredSize : MIN_CAPACITY);
        return capacity > 0 ? capacity : MIN_CAPACITY;
    }

    @Override
    public MqPullReq getResult(DecodeContext context) {
        if (declaredSize >= 0 && declaredSize != queueCount) {
            throw new PbException("queues_size mismatch");
        }
        if (queueIds == null) {
            queueIds = fromSeqs = new long[0];
        } else if (queueIds.length != queueCount) {
            queueIds = Arrays.copyOf(queueIds, queueCount);
            fromSeqs = Arrays.copyOf(fromSeqs, queueCount);
        }
        return this;
    }

    private final class QueueCallback implements PbCallback<Boolean> {
        private long queueId;
        private long fromSeq;

        @Override
        public void begin(int len) {
            queueId = 0;
            fromSeq = 0;
        }

        @Override
        public void readFix64(DecodeContext context, int index, long value) {
            if (index == MqPullReq.IDX_DETAIL_QUEUE_ID) {
                queueId = value;
            } else if (index == MqPullReq.IDX_DETAIL_FROM_SEQ) {
                fromSeq = value;
            }
        }

        @Override
        public Boolean getResult(DecodeContext context) {
            queueIds[queueCount] = queueId;
            fromSeqs[queueCount] = fromSeq;
            return Boolean.TRUE;
        }
    }
}

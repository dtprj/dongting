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

import com.github.dtprj.dongting.codec.ParseUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.dtmq.MqPullReq;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author huangli
 */
public final class MqPullReqCallback extends PbCallback<MqPullReq> {
    private static final int MIN_CAPACITY = 16;

    private final QueueCallback queueCallback = new QueueCallback();
    private MqPullReq req;
    private int declaredSize;
    private int queueCount;

    @Override
    protected void begin(int len) {
        req = new MqPullReq();
        declaredSize = -1;
        queueCount = 0;
    }

    @Override
    public void readVarNumber(int index, long value) {
        switch (index) {
            case MqPullReq.IDX_GROUP_ID:
                req.groupId = (int) value;
                break;
            case MqPullReq.IDX_QUEUES_SIZE:
                if (value < 0 || value > Integer.MAX_VALUE) {
                    throw new PbException("invalid queues_size: " + value);
                }
                declaredSize = (int) value;
                break;
            case MqPullReq.IDX_LIMIT:
                req.limit = (int) value;
                break;
            case MqPullReq.IDX_BYTES_LIMIT:
                req.bytesLimit = (int) value;
                break;
            case MqPullReq.IDX_WAIT_MILLIS:
                req.waitMillis = value;
                break;
        }
    }

    @Override
    public void readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index != MqPullReq.IDX_QUEUES) {
            return;
        }
        if (currentPos == 0) {
            if (req.queueIds == null) {
                int capacity = initialCapacity();
                req.queueIds = new long[capacity];
                req.fromSeqs = new long[capacity];
            } else if (queueCount == req.queueIds.length) {
                int capacity = (int) Math.min((long) queueCount * 2, Integer.MAX_VALUE - 8);
                req.queueIds = Arrays.copyOf(req.queueIds, capacity);
                req.fromSeqs = Arrays.copyOf(req.fromSeqs, capacity);
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
    protected MqPullReq getResult() {
        if (declaredSize >= 0 && declaredSize != queueCount) {
            throw new PbException("queues_size mismatch");
        }
        if (req.queueIds == null) {
            req.queueIds = req.fromSeqs = new long[0];
        } else if (req.queueIds.length != queueCount) {
            req.queueIds = Arrays.copyOf(req.queueIds, queueCount);
            req.fromSeqs = Arrays.copyOf(req.fromSeqs, queueCount);
        }
        MqPullReq result = req;
        req = null;
        return result;
    }

    @Override
    protected void end(boolean success, Throwable ex) {
        req = null;
    }

    private final class QueueCallback extends PbCallback<Boolean> {
        private long queueId;
        private long fromSeq;

        @Override
        protected void begin(int len) {
            queueId = 0;
            fromSeq = 0;
        }

        @Override
        public void readFix64(int index, long value) {
            if (index == MqPullReq.IDX_DETAIL_QUEUE_ID) {
                queueId = value;
            } else if (index == MqPullReq.IDX_DETAIL_FROM_SEQ) {
                fromSeq = value;
            }
        }

        @Override
        protected Boolean getResult() {
            req.queueIds[queueCount] = queueId;
            req.fromSeqs[queueCount] = fromSeq;
            return Boolean.TRUE;
        }
    }
}

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
import com.github.dtprj.dongting.dtmq.codec.MqPullReq;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public final class MqPullReqCallback extends MqPullReq implements PbCallback<MqPullReq> {
    private final QueueCallback queueCallback = new QueueCallback();
    private int queueCount;

    @Override
    public void readVarNumber(DecodeContext context, int index, long value) {
        switch (index) {
            case MqPullReq.IDX_GROUP_ID:
                groupId = (int) value;
                break;
            case MqPullReq.IDX_QUEUES_SIZE:
                if (value <= 0 || value > ParseUtil.MAX_COLLECTION_SIZE) {
                    throw new PbException("invalid queues size: " + value);
                }
                if (queueIds != null) {
                    throw new PbException("duplicate or misplaced queues size field");
                }
                queueIds = new long[(int) value];
                fromSeqs = new long[queueIds.length];
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
                throw new PbException("queues field before queues size");
            }
            if (queueCount >= queueIds.length) {
                throw new PbException("queues count exceeds declared size: " + queueIds.length);
            }
        }
        if (ParseUtil.parseNested(context, buf, fieldLen, currentPos, queueCallback) != null) {
            queueCount++;
        }
    }

    @Override
    public MqPullReq getResult(DecodeContext context) {
        if (queueIds == null) {
            throw new PbException("queues size field missing");
        }
        if (queueCount != queueIds.length) {
            throw new PbException("queues count mismatch: declared=" + queueIds.length + ", actual=" + queueCount);
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

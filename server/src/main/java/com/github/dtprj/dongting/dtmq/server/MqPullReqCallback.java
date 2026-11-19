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
    private int queueCount;
    private boolean parsingNested;

    @Override
    public void readVarNumber(DecodeContext context, int index, long value) {
        if (parsingNested) {
            return;
        }
        switch (index) {
            case MqPullReq.IDX_GROUP_ID:
                groupId = (int) value;
                break;
            case MqPullReq.IDX_QUEUES_SIZE:
                if (queueIds == null) {
                    if (value <= 0 || value > ParseUtil.MAX_COLLECTION_SIZE) {
                        throw new PbException("invalid queues size: " + value);
                    }
                    queueIds = new long[(int) value];
                    fromSeqs = new long[queueIds.length];
                }
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
    public void readFix64(DecodeContext context, int index, long value) {
        if (!parsingNested) {
            return;
        }
        if (index == MqPullReq.IDX_DETAIL_QUEUE_ID) {
            queueIds[queueCount] = value;
        } else if (index == MqPullReq.IDX_DETAIL_FROM_SEQ) {
            fromSeqs[queueCount] = value;
        }
    }

    @Override
    public void readBytes(DecodeContext context, int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (parsingNested && currentPos == 0) {
            // An untouched first chunk makes PbParser skip the whole unknown nested field.
            return;
        }
        if (index != MqPullReq.IDX_QUEUES) {
            return;
        }
        if (currentPos == 0) {
            ParseUtil.checkSizeBefore(queueIds, "queues");
            ParseUtil.checkIndexInArray(queueCount, queueIds.length, "queues");
            parsingNested = true;
        }
        if (ParseUtil.parseNested(context, buf, fieldLen, currentPos, this) != null) {
            queueCount++;
            parsingNested = false;
        }
    }

    @Override
    public MqPullReq getResult(DecodeContext context) {
        if (parsingNested) {
            return this;
        }
        if (queueIds == null) {
            throw new PbException("queues size field missing");
        }
        ParseUtil.checkCountMatch(queueIds.length, queueCount, "queues");
        return this;
    }
}

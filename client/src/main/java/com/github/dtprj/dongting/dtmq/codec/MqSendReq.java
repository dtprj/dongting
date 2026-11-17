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
import com.github.dtprj.dongting.dtmq.DtMsg;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class MqSendReq implements Encodable {
    public static final int IDX_GROUP_ID = 1;
    public static final int IDX_QUEUE_ID = 2;
    public static final int IDX_HEADER_SIZE = 3;
    public static final int IDX_BODY_SIZE = 4;
    public static final int IDX_HEADER = 14;
    public static final int IDX_BODY = 15;

    public int groupId;
    // 1-based; 0 is reserved
    public long queueId;
    // opaque bytes, not interpreted by the state machine
    public byte[] header;
    // not null or empty
    public byte[] body;

    private int encodeSize;
    private int prefixSize;

    public MqSendReq() {
    }

    public MqSendReq(int groupId, DtMsg message) {
        this.groupId = groupId;
        this.queueId = message.queueId;
        this.header = message.header;
        this.body = message.body;
    }

    @Override
    public int actualSize() {
        if (encodeSize == 0) {
            if (body == null || body.length == 0) {
                throw new IllegalArgumentException("message body must not be null or empty");
            }
            encodeSize = prefixSize()
                    + PbUtil.sizeOfBytesField(IDX_HEADER, header)
                    + PbUtil.sizeOfBytesField(IDX_BODY, body);
        }
        return encodeSize;
    }

    private int prefixSize() {
        if (prefixSize == 0) {
            prefixSize = PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupId)
                    + PbUtil.sizeOfFix64Field(IDX_QUEUE_ID, queueId)
                    + PbUtil.sizeOfInt32Field(IDX_HEADER_SIZE, header == null ? 0 : header.length)
                    + PbUtil.sizeOfInt32Field(IDX_BODY_SIZE, body.length);
        }
        return prefixSize;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (destBuffer.remaining() < prefixSize()) {
                    return false;
                }
                PbUtil.writeInt32Field(destBuffer, IDX_GROUP_ID, groupId);
                PbUtil.writeFix64Field(destBuffer, IDX_QUEUE_ID, queueId);
                PbUtil.writeInt32Field(destBuffer, IDX_HEADER_SIZE, header == null ? 0 : header.length);
                PbUtil.writeInt32Field(destBuffer, IDX_BODY_SIZE, body.length);
                context.stage = IDX_BODY_SIZE;
                // fall through
            case IDX_BODY_SIZE:
                if (!EncodeUtil.encode(context, destBuffer, IDX_HEADER, header)) {
                    return false;
                }
                // fall through
            case IDX_HEADER:
                return EncodeUtil.encode(context, destBuffer, IDX_BODY, body);
            default:
                throw new CodecException(context);
        }
    }
}

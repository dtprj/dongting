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
import java.util.List;

/**
 * @author huangli
 */
public class MqSendReq implements Encodable {
    public static final int IDX_GROUP_ID = 1;
    public static final int IDX_MESSAGES_SIZE = 2;
    public static final int IDX_MESSAGES = 15;

    public static final int IDX_MESSAGE_QUEUE_ID = 1;
    public static final int IDX_MESSAGE_HEADER = 2;
    public static final int IDX_MESSAGE_BODY = 3;

    public int groupId;
    // batch writes are not atomic
    public List<DtMsg> messages;

    private int encodeSize;
    private final MessageEncoder messageEncoder = new MessageEncoder();

    public MqSendReq() {
    }

    public MqSendReq(int groupId, List<DtMsg> messages) {
        this.groupId = groupId;
        this.messages = messages;
    }

    @Override
    public int actualSize() {
        if (encodeSize == 0) {
            int size = PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupId);
            int count = messages == null ? 0 : messages.size();
            size += PbUtil.sizeOfInt32Field(IDX_MESSAGES_SIZE, count);
            for (int i = 0; i < count; i++) {
                int messageSize = messageSize(messages.get(i));
                size += PbUtil.sizeOfLenFieldPrefix(IDX_MESSAGES, messageSize) + messageSize;
            }
            encodeSize = size;
        }
        return encodeSize;
    }

    private static int messageSize(DtMsg message) {
        return PbUtil.sizeOfFix64Field(IDX_MESSAGE_QUEUE_ID, message.queueId)
                + PbUtil.sizeOfBytesField(IDX_MESSAGE_HEADER, message.header)
                + PbUtil.sizeOfBytesField(IDX_MESSAGE_BODY, message.body);
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
                int count = messages == null ? 0 : messages.size();
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_MESSAGES_SIZE, count)) {
                    return false;
                }
                // fall through
            case IDX_MESSAGES_SIZE:
                return encodeMessages(context, destBuffer);
            default:
                throw new CodecException(context);
        }
    }

    private boolean encodeMessages(EncodeContext context, ByteBuffer destBuffer) {
        int count = messages == null ? 0 : messages.size();
        if (count == 0) {
            context.stage = IDX_MESSAGES;
            return true;
        }
        // the nested stage is the current message index; the message encoder holds the rest of the state
        EncodeContext mc = context.createOrGetNestedContext(false);
        for (int index = mc.stage; index < count; index++) {
            messageEncoder.msg = messages.get(index);
            if (!EncodeUtil.encode(mc, destBuffer, IDX_MESSAGES, messageEncoder)) {
                return false;
            }
            mc.stage = index + 1;
        }
        context.stage = IDX_MESSAGES;
        return true;
    }

    private static final class MessageEncoder implements Encodable {
        private DtMsg msg;

        @Override
        public int actualSize() {
            return messageSize(msg);
        }

        @Override
        public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
            switch (context.stage) {
                case EncodeContext.STAGE_BEGIN:
                    if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_MESSAGE_QUEUE_ID, msg.queueId)) {
                        return false;
                    }
                    // fall through
                case IDX_MESSAGE_QUEUE_ID:
                    if (!EncodeUtil.encode(context, destBuffer, IDX_MESSAGE_HEADER, msg.header)) {
                        return false;
                    }
                    // fall through
                case IDX_MESSAGE_HEADER:
                    return EncodeUtil.encode(context, destBuffer, IDX_MESSAGE_BODY, msg.body);
                default:
                    throw new CodecException(context);
            }
        }
    }
}

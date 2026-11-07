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

import com.github.dtprj.dongting.dtmq.DtMsg;
import com.github.dtprj.dongting.dtmq.codec.MqSendReq;
import com.github.dtprj.dongting.dtmq.proto.DtMq;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MqSendReqTest {
    private static final byte[] HEADER = {7, 0, -1, 3};

    @Test
    void testFullBuffer() throws Exception {
        MqSendReq req = buildReq();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(req);
        DtMq.MqSendReq pb = DtMq.MqSendReq.parseFrom(buf);
        assertEquals(9, pb.getGroupId());
        assertEquals(3, pb.getMessagesSize());
        assertMessage(pb.getMessages(0), 11, HEADER, new byte[]{1, 2, 3});
        assertMessage(pb.getMessages(1), 22, new byte[0], new byte[]{4, 5});
        assertMessage(pb.getMessages(2), 33, new byte[0], new byte[0]);

        MqSendReqCallback decoded = (MqSendReqCallback) CodecTestUtil.fullBufferDecode(
                buf, new MqSendReqCallback());
        try {
            assertDecoded(decoded);
        } finally {
            decoded.clean();
        }
    }

    @Test
    void testSmallBuffer() {
        MqSendReqCallback decoded = (MqSendReqCallback) CodecTestUtil.smallBufferEncodeAndParse(
                buildReq(), new MqSendReqCallback());
        try {
            assertDecoded(decoded);
        } finally {
            decoded.clean();
        }
    }

    private static MqSendReq buildReq() {
        List<DtMsg> messages = Arrays.asList(
                new DtMsg(11, HEADER, new byte[]{1, 2, 3}),
                new DtMsg(22, null, new byte[]{4, 5}),
                new DtMsg(33, new byte[0], null));
        return new MqSendReq(9, messages);
    }

    private static void assertMessage(DtMq.MqMessage message, long queueId, byte[] header, byte[] body) {
        assertEquals(queueId, message.getQueueId());
        assertArrayEquals(header, message.getMsgHeader().toByteArray());
        assertArrayEquals(body, message.getMsgBody().toByteArray());
    }

    private static void assertDecoded(MqSendReqCallback decoded) {
        assertEquals(9, decoded.groupId);
        assertEquals(3, decoded.messages.size());
        assertEquals(11, decoded.messages.get(0).bizKey);
        assertEquals(HEADER.length, decoded.messages.get(0).bizHeaderLen);
        assertEquals(3, decoded.messages.get(0).bodyLen);
        assertEquals(22, decoded.messages.get(1).bizKey);
        assertEquals(2, decoded.messages.get(1).bodyLen);
        assertEquals(33, decoded.messages.get(2).bizKey);
    }
}

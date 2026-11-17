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
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class MqSendReqTest {
    private static final byte[] HEADER = {7, 0, -1, 3};
    private static final byte[] BODY = {1, 2, 3};

    @Test
    void testFullBuffer() throws Exception {
        MqSendReq req = buildReq();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(req);
        DtMq.MqSendReq pb = DtMq.MqSendReq.parseFrom(buf);
        assertEquals(9, pb.getGroupId());
        assertEquals(11, pb.getQueueId());
        assertEquals(HEADER.length, pb.getHeaderSize());
        assertEquals(BODY.length, pb.getBodySize());
        assertArrayEquals(HEADER, pb.getMsgHeader().toByteArray());
        assertArrayEquals(BODY, pb.getMsgBody().toByteArray());

        MqSendReqCallback decoded = CodecTestUtil.fullBufferDecode(
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

    @Test
    void testRejectEmptyBody() {
        assertThrows(IllegalArgumentException.class,
                () -> new MqSendReq(9, new DtMsg(11, HEADER, null)).actualSize());
        assertThrows(IllegalArgumentException.class,
                () -> new MqSendReq(9, new DtMsg(11, HEADER, new byte[0])).actualSize());
    }

    private static MqSendReq buildReq() {
        return new MqSendReq(9, new DtMsg(11, HEADER, BODY));
    }

    private static void assertDecoded(MqSendReqCallback decoded) {
        assertEquals(9, decoded.groupId);
        RaftReqData data = decoded.data;
        assertNotNull(data);
        assertEquals(11, data.bizKey);
        assertEquals(HEADER.length, data.bizHeaderLen);
        assertEquals(BODY.length, data.bodyLen);
        assertEquals(LogHeader.TYPE_NORMAL, data.type);
    }
}

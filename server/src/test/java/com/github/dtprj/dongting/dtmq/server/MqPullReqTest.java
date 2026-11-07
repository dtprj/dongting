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

import com.github.dtprj.dongting.dtmq.codec.MqPullReq;
import com.github.dtprj.dongting.dtmq.proto.DtMq;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MqPullReqTest {
    private static final long[] QUEUE_IDS = {11, 22, 33};
    private static final long[] FROM_SEQS = {0, 100, Long.MAX_VALUE};

    @Test
    void testFullBuffer() throws Exception {
        MqPullReq req = buildReq();
        ByteBuffer buf = CodecTestUtil.fullBufferEncode(req);
        DtMq.MqPullReq pb = DtMq.MqPullReq.parseFrom(buf);
        assertEquals(7, pb.getGroupId());
        assertEquals(3, pb.getQueuesSize());
        assertEquals(1000, pb.getLimit());
        assertEquals(2048, pb.getBytesLimit());
        assertEquals(3000, pb.getWaitMillis());
        for (int i = 0; i < QUEUE_IDS.length; i++) {
            assertEquals(QUEUE_IDS[i], pb.getQueues(i).getQueueId());
            assertEquals(FROM_SEQS[i], pb.getQueues(i).getFromSeq());
        }

        MqPullReq decoded = CodecTestUtil.fullBufferDecode(buf, new MqPullReqCallback());
        assertEquals(req.groupId, decoded.groupId);
        assertArrayEquals(req.queueIds, decoded.queueIds);
        assertArrayEquals(req.fromSeqs, decoded.fromSeqs);
        assertEquals(req.limit, decoded.limit);
        assertEquals(req.bytesLimit, decoded.bytesLimit);
        assertEquals(req.waitMillis, decoded.waitMillis);
    }

    @Test
    void testSmallBuffer() {
        MqPullReq decoded = (MqPullReq) CodecTestUtil.smallBufferEncodeAndParse(
                buildReq(), new MqPullReqCallback());
        assertArrayEquals(QUEUE_IDS, decoded.queueIds);
        assertArrayEquals(FROM_SEQS, decoded.fromSeqs);
        assertEquals(1000, decoded.limit);
        assertEquals(2048, decoded.bytesLimit);
        assertEquals(3000, decoded.waitMillis);
    }

    private static MqPullReq buildReq() {
        return new MqPullReq(7, QUEUE_IDS, FROM_SEQS, 1000, 2048, 3000);
    }
}

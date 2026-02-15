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
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.raft.impl.DtRaftServer;
import com.github.dtprj.dongting.util.CodecTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class AppendRespTest {

    @Test
    public void testFullBuffer() throws Exception {
        AppendResp resp = new AppendResp();
        resp.groupId = 1;
        resp.term = 10;
        resp.success = true;
        resp.appendCode = 0;
        resp.suggestTerm = 11;
        resp.suggestIndex = 200;

        ByteBuffer buf = CodecTestUtil.simpleEncode(resp);
        DtRaftServer.AppendEntriesResp protoResp = DtRaftServer.AppendEntriesResp.parseFrom(buf);
        compare(resp, protoResp);

        AppendResp.Callback callback = new AppendResp.Callback();
        AppendResp result = CodecTestUtil.fullBufferDecode(buf, callback);
        compare(resp, result);
    }

    private void compare(AppendResp expect, DtRaftServer.AppendEntriesResp proto) {
        Assertions.assertEquals(expect.term, proto.getTerm());
        Assertions.assertEquals(expect.success ? 1 : 0, proto.getSuccess());
        Assertions.assertEquals(expect.appendCode, proto.getAppendCode());
        Assertions.assertEquals(expect.suggestTerm, proto.getSuggestTerm());
        Assertions.assertEquals(expect.suggestIndex, proto.getSuggestIndex());
    }

    private void compare(AppendResp expect, AppendResp result) {
        Assertions.assertEquals(expect.term, result.term);
        Assertions.assertEquals(expect.success, result.success);
        Assertions.assertEquals(expect.appendCode, result.appendCode);
        Assertions.assertEquals(expect.suggestTerm, result.suggestTerm);
        Assertions.assertEquals(expect.suggestIndex, result.suggestIndex);
    }
}

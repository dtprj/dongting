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

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.SmallNoCopyWriteFrame;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class InstallSnapshotResp {
    public int term;
    public boolean success;

    public static class Callback extends PbCallback<InstallSnapshotResp> {
        private final InstallSnapshotResp result = new InstallSnapshotResp();

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    result.term = (int) value;
                    break;
                case 2:
                    result.success = value != 0;
                    break;
            }
            return true;
        }

        @Override
        public InstallSnapshotResp getResult() {
            return result;
        }
    }

    public static class InstallRespWriteFrame extends SmallNoCopyWriteFrame {
        private final InstallSnapshotResp resp;

        public InstallRespWriteFrame(InstallSnapshotResp resp) {
            this.resp = resp;
        }

        @Override
        protected int calcActualBodySize() {
            return PbUtil.accurateUnsignedIntSize(1, resp.term) +
                    PbUtil.accurateUnsignedIntSize(2, resp.success ? 1 : 0);
        }

        @Override
        protected void encodeBody(ByteBuffer buf) {
            PbUtil.writeUnsignedInt32(buf, 1, resp.term);
            PbUtil.writeUnsignedInt32(buf, 2, resp.success ? 1 : 0);
        }
    }
}

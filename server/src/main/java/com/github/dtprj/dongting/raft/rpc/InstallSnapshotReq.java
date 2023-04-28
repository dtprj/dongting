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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
//  uint32 group_id = 1;
//  uint32 term = 2;
//  uint32 leader_id = 3;
//  fixed64 last_included_index = 4;
//  uint32 last_included_term = 5;
//  fixed64 offset = 6;
//  bytes data = 7;
//  bool done = 8;
public class InstallSnapshotReq {
    public int groupId;
    public int term;
    public int leaderId;
    public long lastIncludedIndex;
    public int lastIncludedTerm;
    public long offset;
    public RefBuffer data;
    public boolean done;

    public static class Callback extends PbCallback<InstallSnapshotReq> {
        private final InstallSnapshotReq result = new InstallSnapshotReq();
        private final RefBufferFactory heapPool;

        public Callback(RefBufferFactory heapPool) {
            this.heapPool = heapPool;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    result.groupId = (int) value;
                    break;
                case 2:
                    result.term = (int) value;
                    break;
                case 3:
                    result.leaderId = (int) value;
                    break;
                case 4:
                    result.lastIncludedTerm = (int) value;
                    break;
                case 8:
                    result.done = value != 0;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case 4:
                    result.lastIncludedIndex = value;
                    break;
                case 6:
                    result.offset = value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
            if (index == 7) {
                if (begin) {
                    result.data = heapPool.create(len);
                }
                result.data.getBuffer().put(buf);
                if (end) {
                    result.data.getBuffer().flip();
                }
            }
            return true;
        }

        @Override
        public InstallSnapshotReq getResult() {
            return result;
        }
    }

    public static class WriteFrame extends com.github.dtprj.dongting.net.WriteFrame {

        private final InstallSnapshotReq req;

        public WriteFrame(InstallSnapshotReq req) {
            this.req = req;
        }

        @Override
        protected int calcEstimateBodySize() {
            int x = PbUtil.accurateUnsignedIntSize(1, req.groupId)
                    + PbUtil.accurateUnsignedIntSize(2, req.term)
                    + PbUtil.accurateUnsignedIntSize(3, req.leaderId)
                    + PbUtil.accurateFix64Size(4, req.lastIncludedIndex)
                    + PbUtil.accurateUnsignedIntSize(5, req.lastIncludedTerm)
                    + PbUtil.accurateFix64Size(6, req.offset);
            if (req.data != null && req.data.getBuffer().hasRemaining()) {
                x += PbUtil.accurateLengthDelimitedSize(7, req.data.getBuffer().remaining(), false);
            }
            x += PbUtil.accurateUnsignedIntSize(8, req.done ? 1 : 0);
            return x;
        }

        @Override
        protected void encodeBody(ByteBuffer buf, ByteBufferPool pool) {
            super.writeBodySize(buf, estimateBodySize());
            PbUtil.writeUnsignedInt32(buf, 1, req.groupId);
            PbUtil.writeUnsignedInt32(buf, 2, req.term);
            PbUtil.writeUnsignedInt32(buf, 3, req.leaderId);
            PbUtil.writeFix64(buf, 4, req.lastIncludedIndex);
            PbUtil.writeUnsignedInt32(buf, 5, req.lastIncludedTerm);
            PbUtil.writeFix64(buf, 6, req.offset);
            if (req.data != null && req.data.getBuffer().hasRemaining()) {
                PbUtil.writeLengthDelimitedPrefix(buf, 7, req.data.getBuffer().remaining(), false);
                buf.put(req.data.getBuffer());
            }
            PbUtil.writeUnsignedInt32(buf, 8, req.done ? 1 : 0);
        }

    }
}

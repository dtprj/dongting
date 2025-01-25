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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftConfigRpcData;

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
//  bool done = 7;

//  fixed64 next_write_pos = 8;
//  repeated fixed32 members = 9[packed = false];
//  repeated fixed32 observers = 10[packed = false];
//  repeated fixed32 prepared_members = 11[packed = false];
//  repeated fixed32 prepared_observers = 12[packed = false];
//  fixed64 last_config_change_index = 13;

//  bytes data = 15;
public class InstallSnapshotReq extends RaftConfigRpcData {
    // public int groupId;
    // public int term;
    public int leaderId;
    public long lastIncludedIndex;
    public int lastIncludedTerm;
    public long offset;
    public boolean done;

    public long nextWritePos;
    // public final Set<Integer> members = new HashSet<>();
    // public final Set<Integer> observers = new HashSet<>();
    // public final Set<Integer> preparedMembers = new HashSet<>();
    // public final Set<Integer> preparedObservers = new HashSet<>();
    public long lastConfigChangeIndex;

    public RefBuffer data;

    public void release() {
        if (data != null) {
            data.release();
            data = null;
        }
    }

    public static class Callback extends PbCallback<InstallSnapshotReq> {
        private final InstallSnapshotReq result = new InstallSnapshotReq();

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
                case 5:
                    result.lastIncludedTerm = (int) value;
                    break;
                case 7:
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
                case 8:
                    result.nextWritePos = value;
                    break;
                case 13:
                    result.lastConfigChangeIndex = value;
                    break;
            }
            return true;
        }

        public boolean readFix32(int index, int value) {
            switch (index) {
                case 9:
                    result.members.add(value);
                    break;
                case 10:
                    result.observers.add(value);
                    break;
                case 11:
                    result.preparedMembers.add(value);
                    break;
                case 12:
                    result.preparedObservers.add(value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int len, int currentPos) {
            boolean end = buf.remaining() >= len - currentPos;
            if (index == 15) {
                if (currentPos == 0) {
                    result.data = context.getHeapPool().create(len);
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

    public static class InstallReqWritePacket extends WritePacket {

        private final InstallSnapshotReq req;
        private final int headerSize;
        private final int bufferSize;
        private boolean headerWritten = false;

        public InstallReqWritePacket(InstallSnapshotReq req) {
            this.req = req;
            int x = PbUtil.accurateUnsignedIntSize(1, req.groupId)
                    + PbUtil.accurateUnsignedIntSize(2, req.term)
                    + PbUtil.accurateUnsignedIntSize(3, req.leaderId)
                    + PbUtil.accurateFix64Size(4, req.lastIncludedIndex)
                    + PbUtil.accurateUnsignedIntSize(5, req.lastIncludedTerm)
                    + PbUtil.accurateFix64Size(6, req.offset)
                    + PbUtil.accurateUnsignedIntSize(7, req.done ? 1 : 0)
                    + PbUtil.accurateFix64Size(8, req.nextWritePos);
            x += PbUtil.accurateFix32Size(9, req.members);
            x += PbUtil.accurateFix32Size(10, req.observers);
            x += PbUtil.accurateFix32Size(11, req.preparedMembers);
            x += PbUtil.accurateFix32Size(12, req.preparedObservers);
            x += PbUtil.accurateFix64Size(13, req.lastConfigChangeIndex);

            RefBuffer rb = req.data;
            if (rb != null && rb.getBuffer().hasRemaining()) {
                this.bufferSize = rb.getBuffer().remaining();
                x += PbUtil.accurateLengthDelimitedPrefixSize(15, bufferSize) + bufferSize;
            } else {
                this.bufferSize = 0;
            }
            this.headerSize = x - bufferSize;
        }

        @Override
        protected int calcActualBodySize() {
            return headerSize + bufferSize;
        }

        @Override
        protected boolean encodeBody(EncodeContext context, ByteBuffer dest) {
            if (!headerWritten) {
                if (dest.remaining() >= headerSize) {
                    PbUtil.writeUnsignedInt32(dest, 1, req.groupId);
                    PbUtil.writeUnsignedInt32(dest, 2, req.term);
                    PbUtil.writeUnsignedInt32(dest, 3, req.leaderId);
                    PbUtil.writeFix64(dest, 4, req.lastIncludedIndex);
                    PbUtil.writeUnsignedInt32(dest, 5, req.lastIncludedTerm);
                    PbUtil.writeFix64(dest, 6, req.offset);
                    PbUtil.writeUnsignedInt32(dest, 7, req.done ? 1 : 0);
                    PbUtil.writeFix64(dest, 8, req.nextWritePos);
                    PbUtil.writeFix32(dest, 9, req.members);
                    PbUtil.writeFix32(dest, 10, req.observers);
                    PbUtil.writeFix32(dest, 11, req.preparedMembers);
                    PbUtil.writeFix32(dest, 12, req.preparedObservers);
                    PbUtil.writeFix64(dest, 13, req.lastConfigChangeIndex);
                    if (bufferSize > 0) {
                        PbUtil.writeLengthDelimitedPrefix(dest, 15, bufferSize);
                    }
                    headerWritten = true;
                } else {
                    return false;
                }
            }
            if (bufferSize == 0) {
                return true;
            }
            dest.put(req.data.getBuffer());
            return !req.data.getBuffer().hasRemaining();
        }

        @Override
        protected void doClean() {
            req.release();
        }
    }
}

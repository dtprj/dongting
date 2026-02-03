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
import com.github.dtprj.dongting.common.DtCleanable;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftConfigRpcData;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;

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
public class InstallSnapshotReq extends RaftConfigRpcData implements DtCleanable {

    private static final int IDX_GROUP_ID = 1;
    private static final int IDX_TERM = 2;
    private static final int IDX_LEADER_ID = 3;
    private static final int IDX_LAST_INCLUDED_INDEX = 4;
    private static final int IDX_LAST_INCLUDED_TERM = 5;
    private static final int IDX_OFFSET = 6;
    private static final int IDX_DONE = 7;
    private static final int IDX_NEXT_WRITE_POS = 8;
    private static final int IDX_MEMBERS = 9;
    private static final int IDX_OBSERVERS = 10;
    private static final int IDX_PREPARED_MEMBERS = 11;
    private static final int IDX_PREPARED_OBSERVERS = 12;
    private static final int IDX_LAST_CONFIG_CHANGE_INDEX = 13;
    private static final int IDX_DATA = 15;
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

    @Override
    public void clean() {
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
                case IDX_GROUP_ID:
                    result.groupId = (int) value;
                    break;
                case IDX_TERM:
                    result.term = (int) value;
                    break;
                case IDX_LEADER_ID:
                    result.leaderId = (int) value;
                    break;
                case IDX_LAST_INCLUDED_TERM:
                    result.lastIncludedTerm = (int) value;
                    break;
                case IDX_DONE:
                    result.done = value != 0;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case IDX_LAST_INCLUDED_INDEX:
                    result.lastIncludedIndex = value;
                    break;
                case IDX_OFFSET:
                    result.offset = value;
                    break;
                case IDX_NEXT_WRITE_POS:
                    result.nextWritePos = value;
                    break;
                case IDX_LAST_CONFIG_CHANGE_INDEX:
                    result.lastConfigChangeIndex = value;
                    break;
            }
            return true;
        }

        public boolean readFix32(int index, int value) {
            switch (index) {
                case IDX_MEMBERS:
                    if (result.members == Collections.EMPTY_SET) {
                        result.members = new HashSet<>();
                    }
                    result.members.add(value);
                    break;
                case IDX_OBSERVERS:
                    if (result.observers == Collections.EMPTY_SET) {
                        result.observers = new HashSet<>();
                    }
                    result.observers.add(value);
                    break;
                case IDX_PREPARED_MEMBERS:
                    if (result.preparedMembers == Collections.EMPTY_SET) {
                        result.preparedMembers = new HashSet<>();
                    }
                    result.preparedMembers.add(value);
                    break;
                case IDX_PREPARED_OBSERVERS:
                    if (result.preparedObservers == Collections.EMPTY_SET) {
                        result.preparedObservers = new HashSet<>();
                    }
                    result.preparedObservers.add(value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int len, int currentPos) {
            boolean end = buf.remaining() >= len - currentPos;
            if (index == IDX_DATA) {
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
        protected void end(boolean success) {
            if (!success) {
                result.clean();
            }
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
            int x = PbUtil.sizeOfInt32Field(IDX_GROUP_ID, req.groupId)
                    + PbUtil.sizeOfInt32Field(IDX_TERM, req.term)
                    + PbUtil.sizeOfInt32Field(IDX_LEADER_ID, req.leaderId)
                    + PbUtil.sizeOfFix64Field(IDX_LAST_INCLUDED_INDEX, req.lastIncludedIndex)
                    + PbUtil.sizeOfInt32Field(IDX_LAST_INCLUDED_TERM, req.lastIncludedTerm)
                    + PbUtil.sizeOfFix64Field(IDX_OFFSET, req.offset)
                    + PbUtil.sizeOfInt32Field(IDX_DONE, req.done ? 1 : 0)
                    + PbUtil.sizeOfFix64Field(IDX_NEXT_WRITE_POS, req.nextWritePos);
            x += PbUtil.sizeOfFix32Field(IDX_MEMBERS, req.members);
            x += PbUtil.sizeOfFix32Field(IDX_OBSERVERS, req.observers);
            x += PbUtil.sizeOfFix32Field(IDX_PREPARED_MEMBERS, req.preparedMembers);
            x += PbUtil.sizeOfFix32Field(IDX_PREPARED_OBSERVERS, req.preparedObservers);
            x += PbUtil.sizeOfFix64Field(IDX_LAST_CONFIG_CHANGE_INDEX, req.lastConfigChangeIndex);

            RefBuffer rb = req.data;
            if (rb != null && rb.getBuffer().hasRemaining()) {
                this.bufferSize = rb.getBuffer().remaining();
                x += PbUtil.sizeOfLenFieldPrefix(IDX_DATA, bufferSize) + bufferSize;
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
                    PbUtil.writeInt32Field(dest, IDX_GROUP_ID, req.groupId);
                    PbUtil.writeInt32Field(dest, IDX_TERM, req.term);
                    PbUtil.writeInt32Field(dest, IDX_LEADER_ID, req.leaderId);
                    PbUtil.writeFix64Field(dest, IDX_LAST_INCLUDED_INDEX, req.lastIncludedIndex);
                    PbUtil.writeInt32Field(dest, IDX_LAST_INCLUDED_TERM, req.lastIncludedTerm);
                    PbUtil.writeFix64Field(dest, IDX_OFFSET, req.offset);
                    PbUtil.writeInt32Field(dest, IDX_DONE, req.done ? 1 : 0);
                    PbUtil.writeFix64Field(dest, IDX_NEXT_WRITE_POS, req.nextWritePos);
                    PbUtil.writeFix32Field(dest, IDX_MEMBERS, req.members);
                    PbUtil.writeFix32Field(dest, IDX_OBSERVERS, req.observers);
                    PbUtil.writeFix32Field(dest, IDX_PREPARED_MEMBERS, req.preparedMembers);
                    PbUtil.writeFix32Field(dest, IDX_PREPARED_OBSERVERS, req.preparedObservers);
                    PbUtil.writeFix64Field(dest, IDX_LAST_CONFIG_CHANGE_INDEX, req.lastConfigChangeIndex);
                    if (bufferSize > 0) {
                        PbUtil.writeLenFieldPrefix(dest, IDX_DATA, bufferSize);
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
            req.clean();
        }
    }
}

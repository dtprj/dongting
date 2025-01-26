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
import com.github.dtprj.dongting.codec.SimpleEncodable;
import com.github.dtprj.dongting.raft.RaftConfigRpcData;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * @author huangli
 */
public class AdminPrepareConfigChangeReq extends RaftConfigRpcData implements SimpleEncodable {

    /*
    message AdminPrepareConfigChangeReq {
        uint32 group_id = 1;
        repeated fixed32 members = 2[packed = false];
        repeated fixed32 observers = 3[packed = false];
        repeated fixed32 prepared_members = 4[packed = false];
        repeated fixed32 prepared_observers = 5[packed = false];
        repeated fixed32 new_members = 6[packed = false];
        repeated fixed32 new_observers = 7[packed = false];
    }
     */

    public Set<Integer> newMembers;
    public Set<Integer> newObservers;

    public AdminPrepareConfigChangeReq() {
    }

    @Override
    public int actualSize() {
        return PbUtil.accurateUnsignedIntSize(1, groupId)
                + PbUtil.accurateFix32Size(2, members)
                + PbUtil.accurateFix32Size(3, observers)
                + PbUtil.accurateFix32Size(4, preparedMembers)
                + PbUtil.accurateFix32Size(5, preparedObservers)
                + PbUtil.accurateFix32Size(6, newMembers)
                + PbUtil.accurateFix32Size(7, newObservers);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, groupId);
        PbUtil.writeFix32(buf, 2, members);
        PbUtil.writeFix32(buf, 3, observers);
        PbUtil.writeFix32(buf, 4, preparedMembers);
        PbUtil.writeFix32(buf, 5, preparedObservers);
        PbUtil.writeFix32(buf, 6, newMembers);
        PbUtil.writeFix32(buf, 7, newObservers);
    }

    static final class Callback extends PbCallback<AdminPrepareConfigChangeReq> {
        private final AdminPrepareConfigChangeReq req = new AdminPrepareConfigChangeReq();

        @Override
        protected AdminPrepareConfigChangeReq getResult() {
            return req;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == 1) {
                req.groupId = (int) value;
            }
            return true;
        }

        @Override
        public boolean readFix32(int index, int value) {
            switch (index) {
                case 2:
                    req.members.add(value);
                    break;
                case 3:
                    req.observers.add(value);
                    break;
                case 4:
                    req.preparedMembers.add(value);
                    break;
                case 5:
                    req.preparedObservers.add(value);
                    break;
                case 6:
                    req.newMembers.add(value);
                    break;
                case 7:
                    req.newObservers.add(value);
                    break;
            }
            return true;
        }
    }
}

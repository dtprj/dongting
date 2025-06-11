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
import java.util.Collections;
import java.util.HashSet;

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
    }
     */

    public AdminPrepareConfigChangeReq() {
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt32Field(1, groupId)
                + PbUtil.sizeOfFix32Field(2, members)
                + PbUtil.sizeOfFix32Field(3, observers)
                + PbUtil.sizeOfFix32Field(4, preparedMembers)
                + PbUtil.sizeOfFix32Field(5, preparedObservers);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeInt32Field(buf, 1, groupId);
        PbUtil.writeFix32Field(buf, 2, members);
        PbUtil.writeFix32Field(buf, 3, observers);
        PbUtil.writeFix32Field(buf, 4, preparedMembers);
        PbUtil.writeFix32Field(buf, 5, preparedObservers);
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
                    if (req.members == Collections.EMPTY_SET) {
                        req.members = new HashSet<>();
                    }
                    req.members.add(value);
                    break;
                case 3:
                    if (req.observers == Collections.EMPTY_SET) {
                        req.observers = new HashSet<>();
                    }
                    req.observers.add(value);
                    break;
                case 4:
                    if (req.preparedMembers == Collections.EMPTY_SET) {
                        req.preparedMembers = new HashSet<>();
                    }
                    req.preparedMembers.add(value);
                    break;
                case 5:
                    if (req.preparedObservers == Collections.EMPTY_SET) {
                        req.preparedObservers = new HashSet<>();
                    }
                    req.preparedObservers.add(value);
                    break;
            }
            return true;
        }
    }
}

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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.dtprj.dongting.raft;

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.codec.SimpleEncodable;

import java.nio.ByteBuffer;

/**
 * One way push from raft server to client.
 *
 * @author huangli
 */
//message RaftServerEventPush {
//  int32 group_id = 1;
//  int32 node_id = 2;
//  int32 event_type = 3; // 1 server stop, 2 group stop
//}
public class RaftServerEventPush extends RaftRpcData implements SimpleEncodable {

    public static final int EVENT_SERVER_STOP = 1;
    public static final int EVENT_GROUP_STOP = 2;

    private static final int IDX_GROUP_ID = 1;
    private static final int IDX_NODE_ID = 2;
    private static final int IDX_EVENT_TYPE = 3;

    public int nodeId;
    public int eventType;

    public RaftServerEventPush() {
    }

    public RaftServerEventPush(int groupId, int nodeId, int eventType) {
        this.groupId = groupId;
        this.nodeId = nodeId;
        this.eventType = eventType;
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupId)
                + PbUtil.sizeOfInt32Field(IDX_NODE_ID, nodeId)
                + PbUtil.sizeOfInt32Field(IDX_EVENT_TYPE, eventType);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeInt32Field(buf, IDX_GROUP_ID, groupId);
        PbUtil.writeInt32Field(buf, IDX_NODE_ID, nodeId);
        PbUtil.writeInt32Field(buf, IDX_EVENT_TYPE, eventType);
    }

    public static final class Callback extends PbCallback<RaftServerEventPush> {
        private final RaftServerEventPush push = new RaftServerEventPush();

        @Override
        public void readVarNumber(int index, long value) {
            switch (index) {
                case IDX_GROUP_ID:
                    push.groupId = (int) value;
                    break;
                case IDX_NODE_ID:
                    push.nodeId = (int) value;
                    break;
                case IDX_EVENT_TYPE:
                    push.eventType = (int) value;
                    break;
            }
        }

        @Override
        public RaftServerEventPush getResult() {
            return push;
        }
    }
}

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

import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.server.ReqInfo;

/**
 * @author huangli
 */
public final class ReqInfoEx<T> extends ReqInfo<T> {
    public final RaftGroupImpl raftGroup;

    public ReqInfoEx(ReadPacket<T> reqFrame, ReqContext reqContext, RaftGroupImpl raftGroup) {
        super(reqFrame, reqContext, raftGroup);
        this.raftGroup = raftGroup;
    }

}

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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;

/**
 * @author huangli
 */
public class ReqInfo<T> {
    private final ReadFrame<T> reqFrame;
    private final ChannelContext channelContext;
    private final ReqContext reqContext;
    private final RaftGroup raftGroup;

    boolean invokeCleanUp;

    protected ReqInfo(ReadFrame<T> reqFrame, ChannelContext channelContext,
                      ReqContext reqContext, RaftGroup raftGroup) {
        this.reqFrame = reqFrame;
        this.channelContext = channelContext;
        this.reqContext = reqContext;
        this.raftGroup = raftGroup;
    }

    public ReadFrame<T> getReqFrame() {
        return reqFrame;
    }

    public ChannelContext getChannelContext() {
        return channelContext;
    }

    public ReqContext getReqContext() {
        return reqContext;
    }

    public RaftGroup getRaftGroup() {
        return raftGroup;
    }
}

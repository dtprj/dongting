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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.store.LogHeader;

/**
 * @author huangli
 */
public abstract class RaftInput {
    public final int bizType;
    public final DtTime deadline;
    public final boolean readOnly;
    public final RaftReqData reqData;
    public RaftCallback callback;

    // decoded biz objects
    public Object bizHeader;
    public Object bizBody;

    protected RaftInput(int bizType, RaftReqData reqData, Object bizHeader, Object bizBody,
                        DtTime deadline, boolean readOnly, RaftCallback callback) {
        if (bizType < 0 || bizType > 127) {
            // we use 1 byte to store bizType in raft log
            throw new IllegalArgumentException("bizType must be in [0, 127]");
        }
        this.bizType = bizType;
        this.reqData = reqData;
        this.deadline = deadline;
        this.readOnly = readOnly;
        this.callback = callback;
        this.bizHeader = bizHeader;
        this.bizBody = bizBody;
    }

    public static RaftInput create(int bizType, RaftReqData reqData, Object bizHeader, Object bizBody,
                                   DtTime deadline, boolean readOnly, RaftCallback callback) {
        return new RaftTask(readOnly ? LogHeader.TYPE_LOG_READ : LogHeader.TYPE_NORMAL,
                bizType, reqData, bizHeader, bizBody, deadline, readOnly, callback);
    }
}

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

/**
 * @author huangli
 */
public final class RaftInput {
    private final int bizType;
    private final DtTime deadline;
    private final boolean readOnly;
    private final Object header;
    private final Object body;
    private final int flowControlSize;

    public RaftInput(int bizType, Object header, Object body, DtTime deadline, int flowControlSize) {
        if (bizType < 0 || bizType > 127) {
            // we use 1 byte to store bizType in raft log
            throw new IllegalArgumentException("bizType must be in [0, 127]");
        }
        this.bizType = bizType;
        this.body = body;
        this.header = header;
        this.deadline = deadline;
        this.readOnly = false;
        this.flowControlSize = flowControlSize;
    }

    @SuppressWarnings("unused")
    public RaftInput(int bizType, Object header, Object body, DtTime deadline) {
        this.bizType = bizType;
        this.body = body;
        this.header = header;
        this.deadline = deadline;
        this.readOnly = true;
        this.flowControlSize = 0;
    }

    public int getFlowControlSize() {
        return flowControlSize;
    }

    public DtTime getDeadline() {
        return deadline;
    }

    public Object getBody() {
        return body;
    }

    public Object getHeader() {
        return header;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isReadOnly() {
        return readOnly;
    }

    public int getBizType() {
        return bizType;
    }
}

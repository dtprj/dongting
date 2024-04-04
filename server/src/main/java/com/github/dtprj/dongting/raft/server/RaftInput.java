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

import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.RefCount;

/**
 * @author huangli
 */
public final class RaftInput {
    private final int bizType;
    private final DtTime deadline;
    private final boolean readOnly;
    private final Encodable header;
    private final Encodable body;
    private final long flowControlSize;
    private final boolean headReleasable;
    private final boolean bodyReleasable;

    public RaftInput(int bizType, Encodable header, Encodable body, DtTime deadline) {
        if (bizType < 0 || bizType > 127) {
            // we use 1 byte to store bizType in raft log
            throw new IllegalArgumentException("bizType must be in [0, 127]");
        }
        this.bizType = bizType;
        this.body = body;
        this.header = header;
        this.deadline = deadline;
        this.readOnly = false;
        int flowControlSize = 0;
        if (header != null) {
            flowControlSize += header.actualSize();
        }
        if (body != null) {
            flowControlSize += body.actualSize();
        }
        this.flowControlSize = flowControlSize;
        this.headReleasable = header instanceof RefCount;
        this.bodyReleasable = body instanceof RefCount;
    }

    public long getFlowControlSize() {
        return flowControlSize;
    }

    public DtTime getDeadline() {
        return deadline;
    }

    public Encodable getBody() {
        return body;
    }

    public Encodable getHeader() {
        return header;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public int getBizType() {
        return bizType;
    }

    public boolean isHeadReleasable() {
        return headReleasable;
    }

    public boolean isBodyReleasable() {
        return bodyReleasable;
    }
}

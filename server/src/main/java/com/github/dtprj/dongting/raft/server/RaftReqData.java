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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.common.RefCount;

/**
 * @author huangli
 */
public class RaftReqData extends RefCount {
    public final RefBuffer bizHeader;
    public final int bizHeaderSize;
    public final int bizHeaderCrc;

    public final RefBuffer bizBody;
    public final int bizBodySize;
    public final int bizBodyCrc;

    public final int totalSize;

    public RaftReqData(RefBuffer bizHeader, int bizHeaderCrc, RefBuffer bizBody, int bizBodyCrc) {
        super(false, (bizHeader == null || bizHeader.isDummy()) && (bizBody == null || bizBody.isDummy()));
        this.bizHeader = bizHeader;
        this.bizHeaderCrc = bizHeaderCrc;
        this.bizBody = bizBody;
        this.bizBodyCrc = bizBodyCrc;
        this.bizHeaderSize = bizHeader == null ? 0 : bizHeader.actualSize();
        this.bizBodySize = bizBody == null ? 0 : bizBody.actualSize();
        this.totalSize = (bizHeaderSize == 0 ? 0 : bizHeaderSize + 4) + (bizBodySize == 0 ? 0 : bizBodySize + 4);
    }

    @Override
    protected void doClean() {
        if (bizHeader != null) {
            bizHeader.release();
        }
        if (bizBody != null) {
            bizBody.release();
        }
    }
}

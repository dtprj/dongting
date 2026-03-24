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
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.RefCount;

/**
 * @author huangli
 */
public class RaftReqData extends RefCount {
    public final Encodable bizHeader;
    public final int bizHeaderSize;
    public final int bizHeaderCrc; // reserved, 0 means the crc is not computed

    public final Encodable bizBody;
    public final int bizBodySize;
    public final int bizBodyCrc; // reserved, 0 means the crc is not computed

    public final int totalSize;

    public RaftReqData(Encodable bizHeader, int bizHeaderSize, int bizHeaderCrc,
                       Encodable bizBody, int bizBodySize, int bizBodyCrc) {
        super(false, !(bizHeader instanceof RefCount || bizBody instanceof RefCount));
        this.bizHeader = bizHeader;
        this.bizHeaderSize = bizHeaderSize;
        this.bizHeaderCrc = bizHeaderCrc;
        this.bizBody = bizBody;
        this.bizBodySize = bizBodySize;
        this.bizBodyCrc = bizBodyCrc;
        this.totalSize = (bizHeaderSize == 0 ? 0 : bizHeaderSize + 4) + (bizBodySize == 0 ? 0 : bizBodySize + 4);
    }

    public RaftReqData(Encodable bizHeader, Encodable bizBody) {
        this(bizHeader, bizHeader == null ? 0 : bizHeader.actualSize(), 0,
                bizBody, bizBody == null ? 0 : bizBody.actualSize(), 0);
    }

    public RaftReqData(byte[] bizHeader, byte[] bizBody) {
        this(bizHeader == null ? null : new ByteArray(bizHeader), bizBody == null ? null : new ByteArray(bizBody));
    }

    @Override
    protected void doClean() {
        if (bizHeader instanceof RefCount) {
            ((RefCount) bizHeader).release();
        }
        if (bizBody instanceof RefCount) {
            ((RefCount) bizBody).release();
        }
    }
}

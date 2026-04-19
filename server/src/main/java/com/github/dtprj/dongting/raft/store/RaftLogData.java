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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.raft.server.RaftReqData;

/**
 * @author huangli
 */
public class RaftLogData extends RaftReqData {
    public int totalLen;
    public int bizHeaderLen;
    public int bodyLen;
    public int type;
    public int bizType;
    public int term;
    public int prevLogTerm;
    public long index;
    public long timestamp;
    public int crc;

    public RaftLogData(LogHeader header, RefBuffer bizHeader, int bizHeaderCrc, RefBuffer bizBody, int bizBodyCrc) {
        super(bizHeader, bizHeaderCrc, bizBody, bizBodyCrc);
        this.totalLen = header.totalLen;
        this.bizHeaderLen = header.bizHeaderLen;
        this.bodyLen = header.bodyLen;
        this.type = header.type;
        this.bizType = header.bizType;
        this.term = header.term;
        this.prevLogTerm = header.prevLogTerm;
        this.index = header.index;
        this.timestamp = header.timestamp;
        this.crc = header.headerCrc;
    }

}

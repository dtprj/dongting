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
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.common.RefCount;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.store.LogHeader;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class RaftReqData extends RefCount {
    public final RefBuffer buffer;
    public final LogHeader logHeader;
    public final int totalLen;

    public RaftReqData(LogHeader logHeader, RefBuffer fullBuffer) {
        super(false, fullBuffer.isDummy());
        this.logHeader = logHeader;
        this.buffer = fullBuffer;
        this.totalLen = fullBuffer.actualSize();
    }

    public ByteBuffer prepareReadBizHeader() {
        int bizHeaderLen = logHeader.bizHeaderLen;
        if (bizHeaderLen == 0) {
            return null;
        }
        ByteBuffer buf = buffer.getBuffer();
        buf.limit(totalLen);
        buf.position(LogHeader.ITEM_HEADER_SIZE);
        buf.limit(LogHeader.ITEM_HEADER_SIZE + bizHeaderLen);
        return buf;
    }

    public ByteBuffer prepareReadBizBody() {
        int bizHeaderLen = logHeader.bizHeaderLen;
        int bodyLen = logHeader.bodyLen;
        if (bodyLen == 0) {
            return null;
        }
        int start = LogHeader.ITEM_HEADER_SIZE
                + (bizHeaderLen > 0 ? bizHeaderLen + 4 : 0);
        ByteBuffer buf = buffer.getBuffer();
        buf.limit(totalLen);
        buf.position(start);
        buf.limit(start + bodyLen);
        return buf;
    }

    public void reset() {
        ByteBuffer buf = buffer.getBuffer();
        buf.position(0);
        buf.limit(totalLen);
    }

    public static RaftReqData build(int type, int bizType) {
        LogHeader logHeader = new LogHeader(type);
        logHeader.bizType = bizType;
        logHeader.totalLen = LogHeader.ITEM_HEADER_SIZE;

        ByteBuffer buf = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE);
        RefBuffer refBuffer = RefBuffer.wrap(buf);
        refBuffer.prepareForEncode();
        return new RaftReqData(logHeader, refBuffer);
    }

    public static RaftReqData build(int type, int bizType, Encodable bizBody) {
        if (bizBody == null) {
            return build(type, bizType);
        }
        int bodyLen = bizBody.actualSize();
        if (bodyLen == 0) {
            return build(type, bizType);
        }

        int totalLen = LogHeader.computeTotalLen(0, bodyLen);
        LogHeader logHeader = new LogHeader(type);
        logHeader.bizType = bizType;
        logHeader.bodyLen = bodyLen;
        logHeader.totalLen = totalLen;

        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        buf.position(LogHeader.ITEM_HEADER_SIZE);
        int dataStart = LogHeader.ITEM_HEADER_SIZE;
        EncodeContext c = new EncodeContext(null);
        boolean finished = bizBody.encode(c, buf);
        if (!finished) {
            throw new IllegalStateException("encode not finished, actualSize=" + bodyLen);
        }
        buf.position(dataStart + bodyLen);
        CRC32C crc = new CRC32C();
        RaftUtil.updateCrc(crc, buf, dataStart, bodyLen);
        buf.putInt((int) crc.getValue());

        buf.flip();
        RefBuffer refBuffer = RefBuffer.wrap(buf);
        refBuffer.prepareForEncode();
        return new RaftReqData(logHeader, refBuffer);
    }

    @Override
    protected void doClean() {
        if (buffer != null) {
            buffer.release();
        }
    }
}

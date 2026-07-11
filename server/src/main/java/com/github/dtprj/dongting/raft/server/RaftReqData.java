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

import com.github.dtprj.dongting.buf.Buffers;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.common.DtBugException;
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
    public final int totalLen;

    public int bizHeaderLen;
    public int bodyLen;
    public int type;
    public int bizType;
    public int term;
    public int prevLogTerm;
    public long index;
    public long timestamp;

    public RaftReqData(RefBuffer fullBuffer) {
        super(false, fullBuffer.isDummy());
        this.buffer = fullBuffer;
        this.totalLen = fullBuffer.actualSize();
    }

    public RaftReqData(LogHeader header, RefBuffer fullBuffer) {
        super(false, fullBuffer.isDummy());
        this.buffer = fullBuffer;
        this.totalLen = fullBuffer.actualSize();
        this.bizHeaderLen = header.bizHeaderLen;
        this.bodyLen = header.bodyLen;
        this.type = header.type;
        this.bizType = header.bizType;
        this.term = header.term;
        this.prevLogTerm = header.prevLogTerm;
        this.index = header.index;
        this.timestamp = header.timestamp;
    }

    public ByteBuffer prepareReadBizHeader() {
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

    private static void checkBizType(int bizType) {
        if (bizType < 0 || bizType > 127) {
            // we use 1 byte to store bizType in raft log
            throw new IllegalArgumentException("bizType must be in [0, 127]");
        }
    }

    public static RaftReqData build(int type, int bizType) {
        checkBizType(bizType);
        ByteBuffer buf = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE);
        RefBuffer refBuffer = RefBuffer.wrap(buf);
        refBuffer.prepareForEncode();
        RaftReqData data = new RaftReqData(refBuffer);
        data.type = type;
        data.bizType = bizType;
        return data;
    }

    public static RaftReqData build(int type, int bizType, Encodable bizBody) {
        return build(null, type, bizType, bizBody);
    }

    public static RaftReqData build(Buffers buffers, int type, int bizType, Encodable bizBody) {
        checkBizType(bizType);
        if (bizBody == null) {
            return build(type, bizType);
        }
        int bodyLen = bizBody.actualSize();
        if (bodyLen == 0) {
            return build(type, bizType);
        }

        int totalLen = LogHeader.computeTotalLen(0, bodyLen);

        ByteBuffer buf;
        RefBuffer refBuffer;
        if (buffers == null) {
            buf = ByteBuffer.allocate(totalLen);
            refBuffer = RefBuffer.wrap(buf);
        } else {
            refBuffer = buffers.borrow(totalLen);
            buf = refBuffer.getBuffer();
        }
        int bodyStart = LogHeader.ITEM_HEADER_SIZE;
        int bodyEnd = bodyStart + bodyLen;
        buf.position(bodyStart);
        EncodeContext c = new EncodeContext(null);
        CRC32C crc = new CRC32C();
        int chunkStart = bodyStart;
        while (true) {
            int chunkEnd = chunkStart + RaftServerConfig.ENCODE_CHUNK_SIZE;
            buf.limit(Math.min(chunkEnd, bodyEnd));
            boolean finished = bizBody.encode(c, buf);
            int pos = buf.position();
            RaftUtil.updateCrc(crc, buf, chunkStart, pos - chunkStart);
            if (finished) {
                if (pos != bodyEnd) {
                    throw new DtBugException("encode finished at wrong position: " + pos);
                }
                break;
            } else {
                if (pos >= bodyEnd) {
                    throw new DtBugException("encode not finished at expected position: " + bodyEnd);
                }
                if (chunkEnd >= bodyEnd) {
                    throw new DtBugException("encode not finished when dest buffer has enough space");
                }
            }
            chunkStart = pos;
        }
        buf.limit(totalLen);
        buf.putInt((int) crc.getValue());
        buf.flip();
        refBuffer.prepareForEncode();
        RaftReqData data = new RaftReqData(refBuffer);
        data.type = type;
        data.bizType = bizType;
        data.bodyLen = bodyLen;
        return data;
    }

    @Override
    protected void doClean() {
        if (buffer != null) {
            buffer.release();
        }
    }
}

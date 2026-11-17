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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.common.DtCleanable;
import com.github.dtprj.dongting.dtmq.codec.MqSendReq;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.store.LogHeader;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * Decoder of {@link MqSendReq}, the result holds the log item to append.
 *
 * @author huangli
 */
final class MqSendReqCallback implements PbCallback<MqSendReqCallback>, DtCleanable {
    private final CRC32C crc = new CRC32C();
    int groupId;
    RaftReqData data;

    private long queueId;
    private int headerLen;
    private int bodyLen;
    private int requestLen;
    private int lastFieldIndex;

    @Override
    public void begin(int len) {
        requestLen = len;
    }

    @Override
    public void readVarNumber(DecodeContext context, int index, long value) {
        switch (index) {
            case MqSendReq.IDX_GROUP_ID:
                groupId = (int) value;
                break;
            case MqSendReq.IDX_HEADER_SIZE:
                checkOrder(index);
                headerLen = parseSize(value);
                break;
            case MqSendReq.IDX_BODY_SIZE:
                checkOrder(index);
                bodyLen = parseSize(value);
                if (bodyLen == 0) {
                    throw new CodecException("message body must not be empty");
                }
                allocate(context);
                break;
        }
    }

    @Override
    public void readFix64(DecodeContext context, int index, long value) {
        if (index == MqSendReq.IDX_QUEUE_ID) {
            checkOrder(index);
            queueId = value;
        }
    }

    private int parseSize(long value) {
        if (value < 0 || value > requestLen) {
            throw new CodecException("invalid message content size: " + value);
        }
        return (int) value;
    }

    private void checkOrder(int index) {
        if (index <= lastFieldIndex) {
            throw new CodecException("out of order or duplicate field: " + index);
        }
        lastFieldIndex = index;
    }

    @Override
    public void readBytes(DecodeContext context, int index, ByteBuffer src, int fieldLen, int currentPos) {
        if (index != MqSendReq.IDX_HEADER && index != MqSendReq.IDX_BODY) {
            return;
        }
        if (currentPos == 0) {
            checkOrder(index);
            int declaredLen = index == MqSendReq.IDX_HEADER ? headerLen : bodyLen;
            if (fieldLen != declaredLen) {
                throw new CodecException("message content length mismatch: declared "
                        + declaredLen + ", actual " + fieldLen);
            }
            if (data == null) {
                throw new CodecException("body_size field missing");
            }
            crc.reset();
        }
        ByteBuffer dest = data.buffer.getBuffer();
        int len = src.remaining();
        while (src.hasRemaining()) {
            int chunk = Math.min(src.remaining(), RaftServerConfig.ENCODE_CHUNK_SIZE);
            int oldLimit = src.limit();
            int start = dest.position();
            src.limit(src.position() + chunk);
            dest.put(src);
            src.limit(oldLimit);
            RaftUtil.updateCrc(crc, dest, start, chunk);
        }
        if (currentPos + len == fieldLen && fieldLen > 0) {
            dest.putInt((int) crc.getValue());
        }
    }

    private void allocate(DecodeContext context) {
        if ((long) headerLen + bodyLen > requestLen) {
            throw new CodecException("declared content size exceeds message size");
        }
        int totalLen = LogHeader.computeTotalLen(headerLen, bodyLen);
        RefBuffer buffer = totalLen >= RaftServerConfig.GATHERING_WRITE_THRESHOLD
                ? context.buffers.borrowDirect(totalLen) : context.buffers.borrow(totalLen, false, true);
        ByteBuffer buf = buffer.getBuffer();
        buf.limit(totalLen);
        buffer.prepareForEncode();
        buf.position(LogHeader.ITEM_HEADER_SIZE);
        RaftReqData data = new RaftReqData(buffer);
        data.type = LogHeader.TYPE_NORMAL;
        data.bodyLen = bodyLen;
        data.bizHeaderLen = headerLen;
        data.bizKey = queueId;
        this.data = data;
    }

    @Override
    public MqSendReqCallback getResult(DecodeContext context) {
        RaftReqData data = this.data;
        if (data == null) {
            throw new CodecException("body_size field missing");
        }
        ByteBuffer buf = data.buffer.getBuffer();
        if (buf.position() != LogHeader.computeTotalLen(headerLen, bodyLen)) {
            throw new CodecException("message content does not match declared sizes");
        }
        buf.flip();
        return this;
    }

    @Override
    public void end(boolean success, Throwable ex) {
        if (!success) {
            clean();
        }
    }

    @Override
    public void clean() {
        RaftReqData data = this.data;
        if (data != null) {
            this.data = null;
            data.release();
        }
    }
}

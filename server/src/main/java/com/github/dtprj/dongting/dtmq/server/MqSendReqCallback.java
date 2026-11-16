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
import com.github.dtprj.dongting.codec.ParseUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.common.DtCleanable;
import com.github.dtprj.dongting.dtmq.codec.MqSendReq;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.store.LogHeader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.zip.CRC32C;

/**
 * Decoder of {@link MqSendReq}, the result holds one log item per message.
 *
 * @author huangli
 */
final class MqSendReqCallback implements PbCallback<MqSendReqCallback>, DtCleanable {
    private final MessageCallback messageCallback = new MessageCallback();
    int groupId;
    ArrayList<RaftReqData> messages;
    private int declaredSize;

    @Override
    public void readVarNumber(DecodeContext context, int index, long value) {
        switch (index) {
            case MqSendReq.IDX_GROUP_ID:
                groupId = (int) value;
                break;
            case MqSendReq.IDX_MESSAGES_SIZE:
                declaredSize = (int) value;
                break;
        }
    }

    @Override
    public void readBytes(DecodeContext context, int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index == MqSendReq.IDX_MESSAGES) {
            if (messages == null) {
                messages = ParseUtil.createArrayList(declaredSize > 0 ? declaredSize : 4);
            }
            RaftReqData data = ParseUtil.parseNested(context, buf, fieldLen, currentPos, messageCallback);
            if (data != null) {
                messages.add(data);
            }
        }
    }

    @Override
    public MqSendReqCallback getResult(DecodeContext context) {
        return this;
    }

    @Override
    public void end(boolean success, Throwable ex) {
        messageCallback.releaseBuffer();
        if (!success) {
            clean();
        }
    }

    @Override
    public void clean() {
        if (messages != null) {
            for (RaftReqData data : messages) {
                if (data != null) {
                    data.release();
                }
            }
            messages = null;
        }
    }

    private static final class MessageCallback implements PbCallback<RaftReqData> {
        private final CRC32C crc = new CRC32C();
        private long queueId;
        private int messageLen;
        private int headerLen;
        private int bodyLen;
        private int lastFieldIndex;
        private RefBuffer buffer;

        @Override
        public void begin(int len) {
            queueId = 0;
            messageLen = len;
            headerLen = 0;
            bodyLen = 0;
            lastFieldIndex = 0;
        }

        @Override
        public void readFix64(DecodeContext context, int index, long value) {
            if (index == MqSendReq.IDX_MESSAGE_QUEUE_ID) {
                checkOrder(index);
                queueId = value;
            }
        }

        @Override
        public void readVarNumber(DecodeContext context, int index, long value) {
            switch (index) {
                case MqSendReq.IDX_MESSAGE_HEADER_SIZE:
                    checkOrder(index);
                    headerLen = parseSize(value);
                    break;
                case MqSendReq.IDX_MESSAGE_BODY_SIZE:
                    checkOrder(index);
                    bodyLen = parseSize(value);
                    if (bodyLen == 0) {
                        throw new CodecException("message body must not be empty");
                    }
                    allocate(context);
                    break;
            }
        }

        private int parseSize(long value) {
            if (value < 0 || value > messageLen) {
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
            if (index != MqSendReq.IDX_MESSAGE_HEADER && index != MqSendReq.IDX_MESSAGE_BODY) {
                return;
            }
            if (currentPos == 0) {
                checkOrder(index);
                int declaredLen = index == MqSendReq.IDX_MESSAGE_HEADER ? headerLen : bodyLen;
                if (fieldLen != declaredLen) {
                    throw new CodecException("message content length mismatch: declared "
                            + declaredLen + ", actual " + fieldLen);
                }
                if (buffer == null) {
                    throw new CodecException("body_size field missing");
                }
                crc.reset();
            }
            ByteBuffer dest = buffer.getBuffer();
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
            if ((long) headerLen + bodyLen > messageLen) {
                throw new CodecException("declared content size exceeds message size");
            }
            int totalLen = LogHeader.computeTotalLen(headerLen, bodyLen);
            buffer = totalLen >= RaftServerConfig.GATHERING_WRITE_THRESHOLD
                    ? context.buffers.borrowDirect(totalLen) : context.buffers.borrow(totalLen, false, true, 512);
            buffer.getBuffer().position(LogHeader.ITEM_HEADER_SIZE);
        }

        @Override
        public RaftReqData getResult(DecodeContext context) {
            if (buffer == null) {
                throw new CodecException("body_size field missing");
            }
            ByteBuffer buf = buffer.getBuffer();
            if (buf.position() != LogHeader.computeTotalLen(headerLen, bodyLen)) {
                throw new CodecException("message content does not match declared sizes");
            }
            buf.flip();
            buffer.prepareForEncode();
            RaftReqData data = new RaftReqData(buffer);
            data.type = LogHeader.TYPE_NORMAL;
            data.bodyLen = bodyLen;
            data.bizHeaderLen = headerLen;
            data.bizKey = queueId;
            buffer = null;
            return data;
        }

        private void releaseBuffer() {
            if (buffer != null) {
                buffer.release();
                buffer = null;
            }
        }

        @Override
        public void end(boolean success, Throwable ex) {
            releaseBuffer();
        }
    }
}

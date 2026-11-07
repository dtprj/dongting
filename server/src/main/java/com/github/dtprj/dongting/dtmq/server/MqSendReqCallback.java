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
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.ParseUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
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
    private boolean returned;

    @Override
    public void begin(int len) {
        groupId = 0;
        messages = null;
        declaredSize = -1;
        returned = false;
    }

    @Override
    public void readVarNumber(DecodeContext context, int index, long value) {
        switch (index) {
            case MqSendReq.IDX_GROUP_ID:
                groupId = (int) value;
                break;
            case MqSendReq.IDX_MESSAGES_SIZE:
                if (value < 0 || value > Integer.MAX_VALUE) {
                    throw new PbException("invalid messages_size: " + value);
                }
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
            ParseUtil.parseNested(context, buf, fieldLen, currentPos, messageCallback);
        }
    }

    @Override
    public MqSendReqCallback getResult(DecodeContext context) {
        int size = messages == null ? 0 : messages.size();
        if (declaredSize >= 0 && declaredSize != size) {
            throw new PbException("messages_size mismatch");
        }
        // getResult() throwing must leave returned false, so that end() releases the decoded items
        returned = true;
        return this;
    }

    @Override
    public void end(boolean success, Throwable ex) {
        messageCallback.releaseBuffer();
        if (!returned) {
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

    private final class MessageCallback implements PbCallback<Boolean> {
        private final CRC32C crc = new CRC32C();
        private long queueId;
        private byte[] msgHeader;
        private int bodyLen;
        private RefBuffer buffer;
        // the header already written into buffer, null if the buffer holds the body only
        private byte[] bakedHeader;

        @Override
        public void begin(int len) {
            queueId = 0;
            msgHeader = null;
            bodyLen = 0;
            bakedHeader = null;
        }

        @Override
        public void readFix64(DecodeContext context, int index, long value) {
            if (index == MqSendReq.IDX_MESSAGE_QUEUE_ID) {
                queueId = value;
            }
        }

        @Override
        public void readBytes(DecodeContext context, int index, ByteBuffer src, int fieldLen, int currentPos) {
            if (index == MqSendReq.IDX_MESSAGE_HEADER) {
                msgHeader = ParseUtil.parseBytes(context, src, fieldLen, currentPos);
            } else if (index == MqSendReq.IDX_MESSAGE_BODY) {
                if (currentPos == 0) {
                    startBody(context, fieldLen);
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
                if (currentPos + len == fieldLen) {
                    if (fieldLen > 0) {
                        dest.putInt((int) crc.getValue());
                    }
                    dest.flip();
                }
            }
        }

        // the header usually arrives before the body, so it is written into the item buffer up front
        private void startBody(DecodeContext context, int fieldLen) {
            releaseBuffer();
            bodyLen = fieldLen;
            buffer = borrowItem(context);
            bakedHeader = msgHeader;
            ByteBuffer dest = buffer.getBuffer();
            dest.position(LogHeader.ITEM_HEADER_SIZE);
            if (bakedHeader != null && bakedHeader.length > 0) {
                writeHeader(dest);
            }
            crc.reset();
        }

        // handles a header that arrives after the body (non-canonical encoding), or a repeated header
        private void relayout(DecodeContext context) {
            RefBuffer full = borrowItem(context);
            try {
                ByteBuffer dest = full.getBuffer();
                dest.position(LogHeader.ITEM_HEADER_SIZE);
                if (msgHeader != null && msgHeader.length > 0) {
                    writeHeader(dest);
                }
                if (bodyLen > 0) {
                    int oldHeaderLen = bakedHeader == null ? 0 : bakedHeader.length;
                    // the body is delimited by the old buffer's limit, its crc included
                    ByteBuffer src = buffer.getBuffer();
                    src.position(LogHeader.ITEM_HEADER_SIZE + (oldHeaderLen == 0 ? 0 : oldHeaderLen + 4));
                    dest.put(src);
                }
                dest.flip();
            } catch (RuntimeException | Error e) {
                full.release();
                throw e;
            }
            releaseBuffer();
            buffer = full;
            bakedHeader = msgHeader;
        }

        private void writeHeader(ByteBuffer dest) {
            dest.put(msgHeader);
            crc.reset();
            crc.update(msgHeader, 0, msgHeader.length);
            dest.putInt((int) crc.getValue());
        }

        private RefBuffer borrowItem(DecodeContext context) {
            int headerLen = msgHeader == null ? 0 : msgHeader.length;
            long size = (long) LogHeader.ITEM_HEADER_SIZE
                    + (headerLen == 0 ? 0 : headerLen + 4L)
                    + (bodyLen == 0 ? 0 : bodyLen + 4L);
            if (size > Integer.MAX_VALUE) {
                throw new PbException("message too large: " + size);
            }
            return size >= RaftServerConfig.GATHERING_WRITE_THRESHOLD
                    ? context.buffers.borrowDirect((int) size) : context.buffers.borrow((int) size, false, true, 512);
        }

        private boolean sameHeader(byte[] a, byte[] b) {
            int la = a == null ? 0 : a.length;
            int lb = b == null ? 0 : b.length;
            return la == lb && (la == 0 || a == b);
        }

        @Override
        public Boolean getResult(DecodeContext context) {
            if (!sameHeader(msgHeader, bakedHeader)) {
                relayout(context);
            }
            RaftReqData data;
            if (buffer == null) {
                data = RaftReqData.build(LogHeader.TYPE_NORMAL, 0);
            } else {
                buffer.prepareForEncode();
                data = new RaftReqData(buffer);
                data.type = LogHeader.TYPE_NORMAL;
                data.bodyLen = bodyLen;
                data.bizHeaderLen = msgHeader == null ? 0 : msgHeader.length;
            }
            data.bizKey = queueId;
            messages.add(data);
            buffer = null;
            return Boolean.TRUE;
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
            msgHeader = null;
            bakedHeader = null;
        }
    }
}

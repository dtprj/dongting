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
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.common.DtCleanable;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.store.LogHeader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.zip.CRC32C;

public final class MqSendReq implements DtCleanable {
    private static final int IDX_GROUP_ID = 1;
    private static final int IDX_MESSAGES_SIZE = 2;
    private static final int IDX_MESSAGES = 15;

    // MqMessage
    private static final int IDX_MESSAGE_QUEUE_ID = 1;
    private static final int IDX_MESSAGE_HEADER = 2;
    private static final int IDX_MESSAGE_BODY = 3;

    int groupId;
    ArrayList<RaftReqData> messages;

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

    public static final class Callback extends PbCallback<MqSendReq> {
        private final MessageCallback messageCallback = new MessageCallback();
        private MqSendReq req;
        private int declaredSize;
        private int maxMessages;

        @Override
        protected void begin(int len) {
            req = new MqSendReq();
            declaredSize = -1;
            maxMessages = len / 2;
        }

        @Override
        public void readVarNumber(int index, long value) {
            if (index == IDX_GROUP_ID) {
                req.groupId = (int) value;
            } else if (index == IDX_MESSAGES_SIZE) {
                if (value < 0 || value > maxMessages) {
                    throw new PbException("invalid messages_size: " + value);
                }
                declaredSize = (int) value;
            }
        }

        @Override
        public void readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == IDX_MESSAGES) {
                if (req.messages == null) {
                    req.messages = new ArrayList<>(declaredSize > 0 ? declaredSize : Math.min(4, maxMessages));
                }
                parseNested(buf, fieldLen, currentPos, messageCallback);
            }
        }

        @Override
        protected MqSendReq getResult() {
            int size = req.messages == null ? 0 : req.messages.size();
            if (declaredSize >= 0 && declaredSize != size) {
                throw new PbException("messages_size mismatch");
            }
            MqSendReq result = req;
            req = null;
            return result;
        }

        @Override
        protected void end(boolean success, Throwable ex) {
            messageCallback.releaseBuffer();
            if (req != null) {
                req.clean();
                req = null;
            }
        }

        private final class MessageCallback extends PbCallback<Boolean> {
            private final CRC32C crc = new CRC32C();
            private long queueId;
            private byte[] msgHeader;
            private int bodyLen;
            private RefBuffer buffer;

            @Override
            protected void begin(int len) {
                queueId = 0;
                bodyLen = 0;
                msgHeader = null;
            }

            @Override
            public void readFix64(int index, long value) {
                if (index == IDX_MESSAGE_QUEUE_ID) {
                    queueId = value;
                }
            }

            @Override
            public void readBytes(int index, ByteBuffer src, int fieldLen, int currentPos) {
                if (index == IDX_MESSAGE_HEADER) {
                    msgHeader = parseBytes(src, fieldLen, currentPos);
                } else if (index == IDX_MESSAGE_BODY) {
                    if (currentPos == 0) {
                        releaseBuffer();
                        if (fieldLen > Integer.MAX_VALUE - LogHeader.ITEM_HEADER_SIZE - 4) {
                            throw new PbException("message body too large: " + fieldLen);
                        }
                        bodyLen = fieldLen;
                        buffer = borrow(LogHeader.computeTotalLen(0, fieldLen));
                        buffer.getBuffer().position(LogHeader.ITEM_HEADER_SIZE);
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
                    if (currentPos + len == fieldLen) {
                        if (fieldLen > 0) {
                            dest.putInt((int) crc.getValue());
                        }
                        dest.flip();
                    }
                }
            }

            private RefBuffer borrow(int size) {
                return size >= RaftServerConfig.GATHERING_WRITE_THRESHOLD
                        ? context.buffers.borrowDirect(size) : context.buffers.borrow(size, false, true, 512);
            }

            private void addHeader() {
                int bodySize = LogHeader.computeTotalLen(0, bodyLen);
                long size = (long) bodySize + msgHeader.length + 4;
                if (size > Integer.MAX_VALUE) {
                    throw new PbException("message too large: " + size);
                }
                RefBuffer full = borrow((int) size);
                try {
                    ByteBuffer dest = full.getBuffer();
                    dest.position(LogHeader.ITEM_HEADER_SIZE);
                    dest.put(msgHeader);
                    crc.reset();
                    crc.update(msgHeader, 0, msgHeader.length);
                    dest.putInt((int) crc.getValue());
                    if (bodyLen > 0) {
                        ByteBuffer src = buffer.getBuffer();
                        src.position(LogHeader.ITEM_HEADER_SIZE);
                        dest.put(src);
                    }
                    dest.flip();
                } catch (RuntimeException | Error e) {
                    full.release();
                    throw e;
                }
                releaseBuffer();
                buffer = full;
            }

            @Override
            protected Boolean getResult() {
                if (msgHeader != null && msgHeader.length > 0) {
                    addHeader();
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
                req.messages.add(data);
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
            protected void end(boolean success, Throwable ex) {
                releaseBuffer();
                msgHeader = null;
            }
        }
    }
}

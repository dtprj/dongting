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
import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.RefBufferDecoderCallback;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.server.RaftReqData;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class RaftLogData extends RaftReqData {
    private static final DtLog log = DtLogs.getLogger(RaftLogData.class);

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

    // re-used
    public static class Callback extends DecoderCallback<Void> {

        private final LogHeader header = new LogHeader();
        private final RefBufferDecoderCallback refBufferCallback = new RefBufferDecoderCallback();
        private final Consumer<RaftLogData> consumer;

        private RefBuffer bizHeader;
        private int bizHeaderCrc;
        private RefBuffer bizBody;
        private int bizBodyCrc;

        private int status;
        private int parsedBytes;

        private final CRC32C crc = new CRC32C();
        private final ByteBuffer tmpBuffer = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE);

        private static final int STATUS_INIT = 0;
        private static final int STATUS_FINISH_HEADER = 1;
        private static final int STATUS_FINISH_BIZ_HEADER = 2;
        private static final int STATUS_FINISH_BIZ_HEADER_CRC = 3;
        private static final int STATUS_FINISH_BIZ_BODY = 4;
        private static final int STATUS_FINISH_BIZ_BODY_CRC = 5;

        public Callback(Consumer<RaftLogData> consumer) {
            this.consumer = consumer;
        }

        private void reset() {
            bizHeader = null;
            bizBody = null;
            bizHeaderCrc = 0;
            bizBodyCrc = 0;
            status = STATUS_INIT;
            parsedBytes = 0;
            tmpBuffer.clear();
        }

        @Override
        protected boolean doDecode(ByteBuffer buffer, int notUsedBodyLen, int notUsedCurrentPos) {
            while (true) {
                int remaining = buffer.remaining();
                if (remaining == 0) {
                    return true;
                }
                switch (status) {
                    case STATUS_INIT: {
                        if (remaining < LogHeader.ITEM_HEADER_SIZE - parsedBytes) {
                            tmpBuffer.put(buffer);
                            parsedBytes += remaining;
                            return true;
                        }
                        if (parsedBytes == 0) {
                            if (!header.readAndCheckCrc(crc, buffer)) {
                                log.error("header crc not match");
                                reset();
                                return false;
                            }
                        } else {
                            int oldLimit = buffer.limit();
                            buffer.limit(buffer.position() + LogHeader.ITEM_HEADER_SIZE - parsedBytes);
                            tmpBuffer.put(buffer);
                            buffer.limit(oldLimit);
                            tmpBuffer.flip();
                            if (!header.readAndCheckCrc(crc, tmpBuffer)) {
                                log.error("header crc not match");
                                reset();
                                return false;
                            }
                            parsedBytes = 0;
                            tmpBuffer.clear();
                        }
                        if (header.bizHeaderLen > 0) {
                            status = STATUS_FINISH_HEADER;
                        } else if (header.bodyLen > 0) {
                            status = STATUS_FINISH_BIZ_HEADER_CRC;
                        } else {
                            consumer.accept(new RaftLogData(header, null, 0, null, 0));
                            reset();
                        }
                        continue;
                    }
                    case STATUS_FINISH_HEADER: {
                        int oldPos = buffer.position();
                        bizHeader = parseNested(buffer, header.bizHeaderLen, parsedBytes, refBufferCallback);
                        if (bizHeader == null) {
                            parsedBytes += buffer.position() - oldPos;
                            return true;
                        } else {
                            parsedBytes = 0;
                        }
                        status = STATUS_FINISH_BIZ_HEADER;
                        continue;
                    }
                    case STATUS_FINISH_BIZ_HEADER: {
                        if (remaining < 4 - parsedBytes) {
                            tmpBuffer.put(buffer);
                            parsedBytes += remaining;
                            return true;
                        }
                        if (parsedBytes > 0) {
                            int oldLimit = buffer.limit();
                            buffer.limit(buffer.position() + 4 - parsedBytes);
                            tmpBuffer.put(buffer);
                            buffer.limit(oldLimit);
                            tmpBuffer.flip();
                            bizHeaderCrc = tmpBuffer.getInt();
                            parsedBytes = 0;
                            tmpBuffer.clear();
                        } else {
                            bizHeaderCrc = buffer.getInt();
                        }
                        if (header.bodyLen > 0) {
                            status = STATUS_FINISH_BIZ_HEADER_CRC;
                        } else {
                            consumer.accept(new RaftLogData(header, bizHeader, bizHeaderCrc, null, 0));
                            reset();
                        }
                        continue;
                    }
                    case STATUS_FINISH_BIZ_HEADER_CRC: {
                        int oldPos = buffer.position();
                        bizBody = parseNested(buffer, header.bodyLen, parsedBytes, refBufferCallback);
                        if (bizBody == null) {
                            parsedBytes += buffer.position() - oldPos;
                            return true;
                        } else {
                            parsedBytes = 0;
                        }
                        status = STATUS_FINISH_BIZ_BODY;
                        continue;
                    }
                    case STATUS_FINISH_BIZ_BODY: {
                        if (remaining < 4 - parsedBytes) {
                            tmpBuffer.put(buffer);
                            parsedBytes += remaining;
                            return true;
                        }
                        if (parsedBytes > 0) {
                            int oldLimit = buffer.limit();
                            buffer.limit(buffer.position() + 4 - parsedBytes);
                            tmpBuffer.put(buffer);
                            buffer.limit(oldLimit);
                            tmpBuffer.flip();
                            bizBodyCrc = tmpBuffer.getInt();
                            parsedBytes = 0;
                            tmpBuffer.clear();
                        } else {
                            bizBodyCrc = buffer.getInt();
                        }
                        status = STATUS_FINISH_BIZ_BODY_CRC;
                        consumer.accept(new RaftLogData(header, bizHeader, bizHeaderCrc, bizBody, bizBodyCrc));
                        reset();
                        continue;
                    }
                    default:
                        throw new CodecException("unknown status: " + status);
                }
            }
        }

        @Override
        protected Void getResult() {
            return null;
        }

        @Override
        protected void end(boolean success) {
            if (!success) {
                if (bizHeader != null) {
                    bizHeader.release();
                }
                if (bizBody != null) {
                    bizBody.release();
                }
            }
            reset();
        }
    }
}

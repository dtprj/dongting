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
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
// re-used
public class RaftLogDataCallback extends DecoderCallback<Void> {

    private final Consumer<RaftReqData> consumer;

    private RefBuffer fullRefBuffer;

    private int status;
    private int parsedBytes;
    private int totalLen;
    private int bizHeaderLen;
    private int bodyLen;

    private final CRC32C crc = new CRC32C();

    private static final int STATUS_INIT = 0;
    private static final int STATUS_FINISH_HEADER = 1;
    private static final int STATUS_FINISH_BIZ_HEADER = 2;
    private static final int STATUS_FINISH_BIZ_HEADER_CRC = 3;
    private static final int STATUS_FINISH_BIZ_BODY = 4;

    public RaftLogDataCallback(Consumer<RaftReqData> consumer) {
        this.consumer = consumer;
    }

    private void reset() {
        fullRefBuffer = null;
        status = STATUS_INIT;
        parsedBytes = 0;
        totalLen = 0;
        bizHeaderLen = 0;
        bodyLen = 0;
    }

    @Override
    protected void doDecode(ByteBuffer src, int notUsedBodyLen, int notUsedCurrentPos) {
        ByteBuffer fullBuffer = fullRefBuffer == null ? null : fullRefBuffer.getBuffer();
        while (true) {
            int remaining = src.remaining();
            if (remaining == 0) {
                return;
            }
            switch (status) {
                case STATUS_INIT: {
                    if (fullRefBuffer == null) {
                        if (parsedBytes == 0 && remaining >= 4) {
                            totalLen = src.getInt();
                        } else {
                            while (parsedBytes < 4) {
                                if (!src.hasRemaining()) {
                                    return;
                                }
                                totalLen = (totalLen << 8) | (src.get() & 0xFF);
                                parsedBytes++;
                            }
                        }
                        fullRefBuffer = totalLen >= RaftServerConfig.GATHERING_WRITE_THRESHOLD ?
                                context.buffers.borrowDirect(totalLen) :
                                context.buffers.borrow(totalLen, false, true, 512);
                        fullBuffer = fullRefBuffer.getBuffer();
                        fullBuffer.putInt(totalLen);
                        parsedBytes = 0;
                    }
                    int headerNeed = LogHeader.ITEM_HEADER_SIZE - fullBuffer.position();
                    if (src.remaining() < headerNeed) {
                        fullBuffer.put(src);
                        return;
                    }
                    int oldLimit = src.limit();
                    src.limit(src.position() + headerNeed);
                    fullBuffer.put(src);
                    src.limit(oldLimit);
                    crc.reset();
                    RaftUtil.updateCrc(crc, fullBuffer, 0, LogHeader.ITEM_HEADER_SIZE - 4);
                    if (fullBuffer.getInt(LogHeader.ITEM_HEADER_SIZE - 4) != (int) crc.getValue()) {
                        throw new CodecException("header crc not match, index unknown");
                    }
                    bizHeaderLen = fullBuffer.getInt(LogHeader.OFFSET_BIZ_HEADER_LEN);
                    bodyLen = fullBuffer.getInt(LogHeader.OFFSET_BODY_LEN);
                    if (bizHeaderLen > 0) {
                        status = STATUS_FINISH_HEADER;
                    } else if (bodyLen > 0) {
                        status = STATUS_FINISH_BIZ_HEADER_CRC;
                    } else {
                        finishRaftReqData(fullBuffer);
                    }
                    continue;
                }
                case STATUS_FINISH_HEADER: {
                    if (parseData(remaining, src, fullBuffer, true)) {
                        status = STATUS_FINISH_BIZ_HEADER;
                        continue;
                    } else {
                        return;
                    }
                }
                case STATUS_FINISH_BIZ_HEADER: {
                    if (parseCrc(remaining, src, fullBuffer)) {
                        int crcValue = fullBuffer.getInt(fullBuffer.position() - 4);
                        if (crcValue != ((int) crc.getValue())) {
                            throw new CodecException("bizHeader crc not match, index=" + fullBuffer.getLong(LogHeader.OFFSET_INDEX)
                                    + ", expected=" + crcValue + ", actual=" + (int) crc.getValue());
                        }
                        if (bodyLen > 0) {
                            status = STATUS_FINISH_BIZ_HEADER_CRC;
                        } else {
                            finishRaftReqData(fullBuffer);
                        }
                        continue;
                    } else {
                        return;
                    }
                }
                case STATUS_FINISH_BIZ_HEADER_CRC: {
                    if (parseData(remaining, src, fullBuffer, false)) {
                        status = STATUS_FINISH_BIZ_BODY;
                        continue;
                    } else {
                        return;
                    }
                }
                case STATUS_FINISH_BIZ_BODY: {
                    if (parseCrc(remaining, src, fullBuffer)) {
                        int crcValue = fullBuffer.getInt(fullBuffer.position() - 4);
                        if (crcValue != ((int) crc.getValue())) {
                            throw new CodecException("bizBody crc not match, index=" + fullBuffer.getLong(LogHeader.OFFSET_INDEX)
                                    + ", expected=" + crcValue + ", actual=" + (int) crc.getValue());
                        }
                        finishRaftReqData(fullBuffer);
                        continue;
                    } else {
                        return;
                    }
                }
                default:
                    throw new CodecException("unknown status: " + status);
            }
        }
    }

    private boolean parseData(int remaining, ByteBuffer src, ByteBuffer fullBuffer, boolean bizHeader) {
        if (parsedBytes == 0) {
            crc.reset();
        }
        int total = bizHeader ? bizHeaderLen : bodyLen;
        int needRead = total - parsedBytes;
        int toRead = Math.min(remaining, needRead);

        int oldLimit = src.limit();
        while (toRead > 0) {
            int chunkSize = Math.min(toRead, RaftServerConfig.ENCODE_CHUNK_SIZE);
            int oldDestPos = fullBuffer.position();

            src.limit(src.position() + chunkSize);
            fullBuffer.put(src);

            RaftUtil.updateCrc(crc, fullBuffer, oldDestPos, chunkSize);
            parsedBytes += chunkSize;
            toRead -= chunkSize;
        }
        src.limit(oldLimit);

        if (parsedBytes >= total) {
            parsedBytes = 0;
            return true;
        }
        return false;
    }

    private boolean parseCrc(int remaining, ByteBuffer src, ByteBuffer fullBuffer) {
        if (parsedBytes == 0 && remaining >= 4) {
            int crcValue = src.getInt();
            fullBuffer.putInt(crcValue);
            return true;
        }
        int needRead = 4 - parsedBytes;
        if (remaining < needRead) {
            fullBuffer.put(src);
            parsedBytes += remaining;
            return false;
        } else {
            for (int i = 0; i < needRead; i++) {
                fullBuffer.put(src.get());
            }
            parsedBytes = 0;
            return true;
        }
    }

    private void finishRaftReqData(ByteBuffer fullBuffer) {
        fullBuffer.flip();
        fullRefBuffer.prepareForEncode();
        RaftReqData reqData = new RaftReqData(fullRefBuffer);
        LogHeader.readFields(fullBuffer, reqData);
        consumer.accept(reqData);
        reset();
    }

    @Override
    protected Void getResult() {
        return null;
    }

    @Override
    protected void end(boolean success) {
        if (!success) {
            if (fullRefBuffer != null) {
                fullRefBuffer.release();
            }
        }
        reset();
    }
}

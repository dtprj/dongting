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

    private ByteBuffer fullBuffer;
    private RefBuffer fullRefBuffer;

    private LogHeader header;
    private int status;
    private int parsedBytes;

    private final CRC32C crc = new CRC32C();
    private final ByteBuffer tmpBuffer = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE);

    private static final int STATUS_INIT = 0;
    private static final int STATUS_FINISH_HEADER = 1;
    private static final int STATUS_FINISH_BIZ_HEADER = 2;
    private static final int STATUS_FINISH_BIZ_HEADER_CRC = 3;
    private static final int STATUS_FINISH_BIZ_BODY = 4;

    public RaftLogDataCallback(Consumer<RaftReqData> consumer) {
        this.consumer = consumer;
    }

    private void reset() {
        fullBuffer = null;
        fullRefBuffer = null;
        header = null;
        status = STATUS_INIT;
        parsedBytes = 0;
        tmpBuffer.clear();
    }

    @Override
    protected void doDecode(ByteBuffer buffer, int notUsedBodyLen, int notUsedCurrentPos) {
        while (true) {
            int remaining = buffer.remaining();
            if (remaining == 0) {
                return;
            }
            switch (status) {
                case STATUS_INIT: {
                    if (remaining < LogHeader.ITEM_HEADER_SIZE - parsedBytes) {
                        tmpBuffer.put(buffer);
                        parsedBytes += remaining;
                        return;
                    }
                    header = new LogHeader();
                    if (parsedBytes == 0) {
                        if (!header.readAndCheckCrc(crc, buffer)) {
                            throw new CodecException("header crc not match, index unknown");
                        }
                    } else {
                        int oldLimit = buffer.limit();
                        buffer.limit(buffer.position() + LogHeader.ITEM_HEADER_SIZE - parsedBytes);
                        tmpBuffer.put(buffer);
                        buffer.limit(oldLimit);
                        tmpBuffer.flip();
                        if (!header.readAndCheckCrc(crc, tmpBuffer)) {
                            throw new CodecException("header crc not match, index unknown");
                        }
                        parsedBytes = 0;
                    }
                    fullRefBuffer = context.buffers.borrowRefBuffer(header.totalLen, false, true, 512);
                    fullBuffer = fullRefBuffer.getBuffer();
                    header.writeTo(fullBuffer);
                    if (header.bizHeaderLen > 0) {
                        status = STATUS_FINISH_HEADER;
                    } else if (header.bodyLen > 0) {
                        status = STATUS_FINISH_BIZ_HEADER_CRC;
                    } else {
                        finishRaftReqData();
                    }
                    continue;
                }
                case STATUS_FINISH_HEADER: {
                    if (parseData(remaining, buffer, true)) {
                        status = STATUS_FINISH_BIZ_HEADER;
                        continue;
                    } else {
                        return;
                    }
                }
                case STATUS_FINISH_BIZ_HEADER: {
                    if (parseCrc(remaining, buffer)) {
                        int crcValue = fullBuffer.getInt(fullBuffer.position() - 4);
                        if (crcValue != ((int) crc.getValue())) {
                            throw new CodecException("bizHeader crc not match, index=" + header.index
                                    + ", expected=" + crcValue + ", actual=" + (int) crc.getValue());
                        }
                        if (header.bodyLen > 0) {
                            status = STATUS_FINISH_BIZ_HEADER_CRC;
                        } else {
                            finishRaftReqData();
                        }
                        continue;
                    } else {
                        return;
                    }
                }
                case STATUS_FINISH_BIZ_HEADER_CRC: {
                    if (parseData(remaining, buffer, false)) {
                        status = STATUS_FINISH_BIZ_BODY;
                        continue;
                    } else {
                        return;
                    }
                }
                case STATUS_FINISH_BIZ_BODY: {
                    if (parseCrc(remaining, buffer)) {
                        int crcValue = fullBuffer.getInt(fullBuffer.position() - 4);
                        if (crcValue != ((int) crc.getValue())) {
                            throw new CodecException("bizBody crc not match, index=" + header.index
                                    + ", expected=" + crcValue + ", actual=" + (int) crc.getValue());
                        }
                        finishRaftReqData();
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

    private boolean parseData(int remaining, ByteBuffer buffer, boolean bizHeader) {
        if (parsedBytes == 0) {
            crc.reset();
        }
        int total = bizHeader ? header.bizHeaderLen : header.bodyLen;
        int needRead = total - parsedBytes;
        int toRead = Math.min(remaining, needRead);

        int oldLimit = buffer.limit();
        while (toRead > 0) {
            int chunkSize = Math.min(toRead, RaftServerConfig.ENCODE_CHUNK_SIZE);
            int oldDestPos = fullBuffer.position();

            buffer.limit(buffer.position() + chunkSize);
            fullBuffer.put(buffer);

            RaftUtil.updateCrc(crc, fullBuffer, oldDestPos, chunkSize);
            parsedBytes += chunkSize;
            toRead -= chunkSize;
        }
        buffer.limit(oldLimit);

        if (parsedBytes >= total) {
            parsedBytes = 0;
            return true;
        }
        return false;
    }

    private boolean parseCrc(int remaining, ByteBuffer buffer) {
        if (parsedBytes == 0 && remaining >= 4) {
            int crcValue = buffer.getInt();
            fullBuffer.putInt(crcValue);
            return true;
        }
        int needRead = 4 - parsedBytes;
        if (remaining < needRead) {
            fullBuffer.put(buffer);
            parsedBytes += remaining;
            return false;
        } else {
            for (int i = 0; i < needRead; i++) {
                fullBuffer.put(buffer.get());
            }
            parsedBytes = 0;
            return true;
        }
    }

    private void finishRaftReqData() {
        fullBuffer.flip();
        fullRefBuffer.prepareForEncode();
        RaftReqData reqData = new RaftReqData(header, fullRefBuffer);
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

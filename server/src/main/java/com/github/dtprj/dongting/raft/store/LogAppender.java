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

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class LogAppender {
    private final IdxOps idxOps;
    private final LogFileQueue logFileQueue;
    private final RaftCodecFactory codecFactory;
    private final ByteBuffer writeBuffer;
    private final CRC32C crc32c = new CRC32C();
    private final EncodeContext encodeContext;
    private final long fileLenMask;

    LogAppender(IdxOps idxOps, LogFileQueue logFileQueue, RaftGroupConfig groupConfig, ByteBuffer writeBuffer) {
        this.idxOps = idxOps;
        this.logFileQueue = logFileQueue;
        this.codecFactory = groupConfig.getCodecFactory();
        this.encodeContext = new EncodeContext(groupConfig.getHeapPool());
        this.writeBuffer = writeBuffer;
        this.fileLenMask = logFileQueue.fileLength() - 1;
    }

    @SuppressWarnings("rawtypes")
    public long append(List<LogItem> logs, long pos) throws IOException, InterruptedException {
        logFileQueue.ensureWritePosReady(pos);
        ByteBuffer writeBuffer = this.writeBuffer;
        writeBuffer.clear();
        long bufferPosOfFile = pos & fileLenMask;
        LogFile file = logFileQueue.getLogFile(pos);
        for (int i = 0; i < logs.size(); i++) {
            LogItem log = logs.get(i);
            Encoder headerEncoder = initEncoderAndSize(log, true);
            Encoder bodyEncoder = initEncoderAndSize(log, false);

            int totalLen = LogHeader.computeTotalLen(0, log.getActualHeaderSize(), log.getActualBodySize());
            if ((pos & fileLenMask) == 0) {
                if (i != 0) {
                    // last item exactly fill the file
                    bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
                    file.channel.force(false);
                    logFileQueue.ensureWritePosReady(pos);
                    file = logFileQueue.getLogFile(pos);
                }
                file.firstTimestamp = log.getTimestamp();
                file.firstIndex = log.getIndex();
                file.firstTerm = log.getTerm();
            } else {
                long fileRest = file.endPos - pos;
                if (fileRest < totalLen) {
                    // file rest space is not enough
                    if (fileRest >= LogHeader.ITEM_HEADER_SIZE) {
                        // write an empty header to indicate the end of the file
                        if (writeBuffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                            bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
                        }
                        LogHeader.writeEndHeader(crc32c, writeBuffer);
                    }
                    writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);

                    // roll to next file
                    file.channel.force(false);
                    pos = logFileQueue.nextFilePos(pos);
                    bufferPosOfFile = pos;
                    logFileQueue.ensureWritePosReady(pos);
                    file = logFileQueue.getLogFile(pos);

                    file.firstTimestamp = log.getTimestamp();
                    file.firstIndex = log.getIndex();
                    file.firstTerm = log.getTerm();
                }
            }

            if (writeBuffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
            }

            long itemStartPos = pos;
            LogHeader.writeHeader(crc32c, writeBuffer, log, 0, log.getActualHeaderSize(), log.getActualBodySize());
            pos += LogHeader.ITEM_HEADER_SIZE;

            if (headerEncoder != null && log.getActualHeaderSize() > 0) {
                Object data = log.getHeaderBuffer() != null ? log.getHeaderBuffer() : log.getHeader();
                bufferPosOfFile = writeData(writeBuffer, bufferPosOfFile, file, data, headerEncoder);
                pos += log.getActualHeaderSize() + 4;
            }
            if (bodyEncoder != null && log.getActualBodySize() > 0) {
                Object data = log.getBodyBuffer() != null ? log.getBodyBuffer() : log.getBody();
                bufferPosOfFile = writeData(writeBuffer, bufferPosOfFile, file, data, bodyEncoder);
                pos += log.getActualBodySize() + 4;
            }

            idxOps.put(log.getIndex(), itemStartPos, false);
        }
        writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
        file.channel.force(false);
        return pos;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Encoder initEncoderAndSize(LogItem item, boolean header) {
        if (header) {
            if (item.getHeaderBuffer() != null) {
                item.setActualHeaderSize(item.getHeaderBuffer().remaining());
                return ByteBufferEncoder.INSTANCE;
            } else if (item.getHeader() != null) {
                Encoder encoder = codecFactory.createHeaderEncoder(item.getBizType());
                item.setActualHeaderSize(encoder.actualSize(item.getHeader()));
                return encoder;
            } else {
                return null;
            }
        } else {
            if (item.getBodyBuffer() != null) {
                item.setActualBodySize(item.getBodyBuffer().remaining());
                return ByteBufferEncoder.INSTANCE;
            } else if (item.getBody() != null) {
                Encoder encoder = codecFactory.createBodyEncoder(item.getBizType());
                item.setActualBodySize(encoder.actualSize(item.getBody()));
                return encoder;
            } else {
                return null;
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private long writeData(ByteBuffer writeBuffer, long bufferPosOfFile, LogFile file, Object data, Encoder encoder) throws IOException {
        crc32c.reset();
        if (writeBuffer.remaining() == 0) {
            bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
        }
        try {
            while (true) {
                int lastPos = writeBuffer.position();
                @SuppressWarnings("unchecked") boolean encodeFinish = encoder.encode(encodeContext, writeBuffer, data);
                if (writeBuffer.position() > lastPos) {
                    RaftUtil.updateCrc(crc32c, writeBuffer, lastPos, writeBuffer.position() - lastPos);
                }
                if (encodeFinish) {
                    break;
                }
                bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
            }
        } finally {
            encodeContext.setStatus(null);
        }

        if (writeBuffer.remaining() < 4) {
            bufferPosOfFile = writeAndClearBuffer(writeBuffer, file, bufferPosOfFile);
        }
        writeBuffer.putInt((int) crc32c.getValue());
        return bufferPosOfFile;
    }

    private long writeAndClearBuffer(ByteBuffer buffer, LogFile file, long pos) throws IOException {
        if (buffer.position() == 0) {
            return pos;
        }
        long posOfFile = pos & fileLenMask;
        buffer.flip();
        int count = buffer.remaining();
        FileUtil.syncWriteFull(file.channel, buffer, posOfFile);
        buffer.clear();
        return pos + count;
    }
}

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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class Restorer {
    private final DtLog log = DtLogs.getLogger(Restorer.class);
    private final CRC32C crc32c = new CRC32C();
    private final IdxOps idxOps;
    private final long commitIndex;
    private final long commitIndexPos;

    private boolean commitIndexChecked;

    private long itemStartPosOfFile;
    private boolean readHeader = true;
    private LogHeader header = new LogHeader();
    private int bodyRestLen;

    long previousIndex;
    int previousTerm;

    public Restorer(IdxOps idxOps, long commitIndex, long commitIndexPos) {
        this.idxOps = idxOps;
        this.commitIndex = commitIndex;
        this.commitIndexPos = commitIndexPos;
    }

    public long restoreFile(ByteBuffer buffer, LogFile lf) throws IOException {
        buffer.clear();
        buffer.limit(LogHeader.ITEM_HEADER_SIZE);
        FileUtil.syncReadFull(lf.channel, buffer, 0);
        buffer.flip();
        LogHeader header = new LogHeader();
        header.read(buffer);
        lf.firstIndex = header.index;
        lf.firstTerm = header.term;
        lf.firstTimestamp = header.timestamp;

        if (commitIndexPos < lf.endPos) {
            long pos = restoreFile0(buffer, lf);
            return lf.startPos + pos;
        } else {
            return lf.endPos;
        }
    }

    private long restoreFile0(ByteBuffer buffer, LogFile lf) throws IOException {
        log.info("try restore file {}", lf.file.getPath());
        if (commitIndexPos >= lf.startPos) {
            // check from commitIndexPos
            itemStartPosOfFile = commitIndexPos & LogFileQueue.FILE_LEN_MASK;
        } else {
            // check full file
            itemStartPosOfFile = 0;
        }
        AsynchronousFileChannel channel = lf.channel;
        long readPos = itemStartPosOfFile;
        buffer.clear();
        while (readPos < LogFileQueue.LOG_FILE_SIZE) {
            int read = FileUtil.syncRead(channel, buffer, readPos);
            if (read <= 0) {
                continue;
            }
            buffer.flip();
            if (restore(buffer, lf, readPos)) {
                LogFileQueue.prepareNextRead(buffer);
                readPos += read;
            } else {
                break;
            }
        }
        return itemStartPosOfFile;
    }

    private boolean restore(ByteBuffer buf, LogFile lf, long readPos) throws IOException {
        while (buf.hasRemaining()) {
            if (readHeader) {
                if (buf.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                    return true;
                }
                int startPos = buf.position();
                header.read(buf);

                if (commitIndexChecked) {
                    if (header.prevLogTerm != previousTerm) {
                        if (header.prevLogTerm == 0 && header.crc == 0) {
                            log.info("reach end of file. file={}, pos={}", lf.file.getPath(), itemStartPosOfFile);
                        } else {
                            log.warn("prevLogTerm not match. file={}, itemStartPos={}, {}!={}",
                                    lf.file.getPath(), itemStartPosOfFile, this.previousTerm, header.prevLogTerm);
                        }
                        return false;
                    }
                    if (this.previousIndex + 1 != header.index) {
                        log.warn("index not match. file={}, itemStartPos={}, {}!={}",
                                lf.file.getPath(), itemStartPosOfFile, this.previousIndex + 1, header.index);
                        return false;
                    }
                    if (header.term < this.previousTerm) {
                        log.warn("term not match. file={}, itemStartPos={}, {}<{}",
                                lf.file.getPath(), itemStartPosOfFile, header.term, this.previousTerm);
                        return false;
                    }
                } else {
                    if (header.index != commitIndex) {
                        throw new RaftException("commitIndex not match. file=" + lf.file.getPath() + ", pos=" + itemStartPosOfFile);
                    }
                    if (header.totalLen <= 0 || header.headLen <= 0 || header.contextLen < 0 ||
                            header.type < 0 || header.term <= 0 || header.prevLogTerm < 0) {
                        throw new RaftException("bad item. file=" + lf.file.getPath() + ", pos=" + itemStartPosOfFile);
                    }
                }

                this.previousTerm = header.term;
                this.previousIndex = header.index;

                if (header.totalLen < header.headLen + header.contextLen
                        || itemStartPosOfFile + header.totalLen > LogFileQueue.LOG_FILE_SIZE) {
                    log.error("bad item len. file={}, itemStartPos={}", lf.file.getPath(), itemStartPosOfFile);
                    return false;
                }

                crc32c.reset();
                LogFileQueue.updateCrc(crc32c, buf, startPos + 4, LogHeader.ITEM_HEADER_SIZE - 4);
                buf.position(startPos + LogHeader.ITEM_HEADER_SIZE);
                readHeader = false;
                bodyRestLen = header.totalLen - header.headLen - header.contextLen;
            } else {
                if (buf.remaining() >= bodyRestLen) {
                    LogFileQueue.updateCrc(crc32c, buf, buf.position(), bodyRestLen);
                    buf.position(buf.position() + bodyRestLen);
                    if (header.crc == crc32c.getValue()) {
                        itemStartPosOfFile += header.totalLen;
                        if (commitIndexChecked) {
                            idxOps.put(this.previousIndex, lf.startPos + itemStartPosOfFile, header.totalLen);
                        } else {
                            commitIndexChecked = true;
                        }
                    } else {
                        log.warn("crc32c not match. file={}, itemStartPos={}", lf.file.getPath(), itemStartPosOfFile);
                        return false;
                    }
                    readHeader = true;
                } else {
                    int rest = buf.remaining();
                    crc32c.update(buf);
                    bodyRestLen -= rest;
                }
            }
        }
        return true;
    }


}

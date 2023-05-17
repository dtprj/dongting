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
import com.github.dtprj.dongting.raft.impl.FileUtil;

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
    private final FileOps fileOps;
    private final long commitIndex;
    private final long commitIndexPos;

    private boolean commitIndexChecked;

    private long itemStartPosOfFile;
    private boolean readHeader = true;
    private final LogHeader header = new LogHeader();
    private int bodyRestLen;

    long previousIndex;
    int previousTerm;

    public Restorer(IdxOps idxOps, FileOps fileOps, long commitIndex, long commitIndexPos) {
        this.idxOps = idxOps;
        this.fileOps = fileOps;
        this.commitIndex = commitIndex;
        this.commitIndexPos = commitIndexPos;
    }

    public long restoreFile(ByteBuffer buffer, LogFile lf) throws IOException {
        buffer.clear();
        buffer.limit(LogHeader.ITEM_HEADER_SIZE);
        FileUtil.syncReadFull(lf.channel, buffer, 0);
        buffer.flip();
        header.read(buffer);
        if (header.crcMatch()) {
            lf.firstIndex = header.index;
            lf.firstTerm = header.term;
            lf.firstTimestamp = header.timestamp;
        }

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
            itemStartPosOfFile = fileOps.filePos(commitIndexPos);
        } else {
            // check full file
            itemStartPosOfFile = 0;
        }
        AsynchronousFileChannel channel = lf.channel;
        long readPos = itemStartPosOfFile;
        buffer.clear();
        while (readPos < fileOps.fileLength()) {
            int read = FileUtil.syncRead(channel, buffer, readPos);
            if (read <= 0) {
                continue;
            }
            buffer.flip();
            if (restore(buffer, lf)) {
                LogFileQueue.prepareNextRead(buffer);
                readPos += read;
            } else {
                break;
            }
        }
        return itemStartPosOfFile;
    }

    private boolean crcFail(LogFile lf) {
        if (commitIndexChecked) {
            log.info("reach end of file. file={}, pos={}", lf.file.getPath(), itemStartPosOfFile);
            return false;
        } else {
            throw new RaftException("commit index crc not match. " + commitIndex + "," + commitIndexPos);
        }
    }

    private boolean restore(ByteBuffer buf, LogFile lf) throws IOException {
        while (buf.hasRemaining()) {
            if (readHeader) {
                if (buf.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                    return true;
                }
                header.read(buf);
                if (!header.crcMatch()) {
                    return crcFail(lf);
                }
                if (!header.checkHeader(itemStartPosOfFile, fileOps.fileLength())) {
                    throw new RaftException("bad item. file=" + lf.file.getPath() + ", pos=" + itemStartPosOfFile);
                }
                if (commitIndexChecked) {
                    if (header.prevLogTerm != previousTerm) {
                        log.warn("prevLogTerm not match. file={}, itemStartPos={}, {}!={}",
                                lf.file.getPath(), itemStartPosOfFile, this.previousTerm, header.prevLogTerm);
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
                    if (header.term <= 0 || header.prevLogTerm < 0) {
                        throw new RaftException("bad item. file=" + lf.file.getPath() + ", pos=" + itemStartPosOfFile);
                    }
                }

                this.previousTerm = header.term;
                this.previousIndex = header.index;
                readHeader = false;
                bodyRestLen = header.totalLen - header.bizHeaderLen;
                crc32c.reset();
            } else {
                if (buf.remaining() >= bodyRestLen + 4) {
                    LogFileQueue.updateCrc(crc32c, buf, buf.position(), bodyRestLen);
                    buf.position(buf.position() + bodyRestLen);
                    if (buf.getInt() == crc32c.getValue()) {
                        itemStartPosOfFile += header.totalLen;
                        if (commitIndexChecked) {
                            idxOps.put(this.previousIndex, lf.startPos + itemStartPosOfFile, header.totalLen);
                        } else {
                            commitIndexChecked = true;
                        }
                    } else {
                        return crcFail(lf);
                    }
                    readHeader = true;
                } else {
                    int rest = buf.remaining();
                    LogFileQueue.updateCrc(crc32c, buf, buf.position(), rest);
                    buf.position(buf.position() + rest);
                    bodyRestLen -= rest;
                    return true;
                }
            }
        }
        return true;
    }


}

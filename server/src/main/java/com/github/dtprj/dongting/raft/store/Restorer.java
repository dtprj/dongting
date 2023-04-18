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
    private int crc;
    private int bodyRestLen;
    private int currentItemLen;

    long previousIndex;
    int previousTerm;
    int totalLen;

    public Restorer(IdxOps idxOps, long commitIndex, long commitIndexPos) {
        this.idxOps = idxOps;
        this.commitIndex = commitIndex;
        this.commitIndexPos = commitIndexPos;
    }

    public long restoreFile(ByteBuffer buffer, LogFile lf) throws IOException {
        log.info("try restore file {}", lf.pathname);
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
            if (restore(buffer, lf)) {
                if (buffer.remaining() == 0) {
                    buffer.clear();
                } else {
                    ByteBuffer temp = buffer.slice();
                    buffer.clear();
                    buffer.put(temp);
                }
            } else {
                break;
            }
        }
        return itemStartPosOfFile;
    }

    private boolean restore(ByteBuffer buf, LogFile lf) throws IOException {
        while (buf.hasRemaining()) {
            if (readHeader) {
                if (buf.remaining() < LogFileQueue.ITEM_HEADER_SIZE) {
                    return true;
                }
                int startPos = buf.position();
                crc = buf.getInt();
                totalLen = buf.getInt();
                short headLen = buf.getShort();
                int contextLen = buf.getInt();// context len
                byte type = buf.get();//type
                int term = buf.getInt();
                int prevLogTerm = buf.getInt();
                long index = buf.getLong();

                if (commitIndexChecked) {
                    if (this.previousTerm != prevLogTerm) {
                        if (prevLogTerm == 0 && crc == 0) {
                            log.info("reach end of file. file={}, pos={}", lf.pathname, itemStartPosOfFile);
                        } else {
                            log.warn("prevLogTerm not match. file={}, itemStartPos={}, {}!={}",
                                    lf.pathname, itemStartPosOfFile, this.previousTerm, prevLogTerm);
                        }
                        return false;
                    }
                    if (this.previousIndex + 1 != index) {
                        log.warn("index not match. file={}, itemStartPos={}, {}!={}",
                                lf.pathname, itemStartPosOfFile, this.previousIndex + 1, index);
                        return false;
                    }
                    if (term < this.previousTerm) {
                        log.warn("term not match. file={}, itemStartPos={}, {}<{}",
                                lf.pathname, itemStartPosOfFile, term, this.previousTerm);
                        return false;
                    }
                } else {
                    if (index != commitIndex) {
                        throw new RaftException("commitIndex not match. file=" + lf.pathname + ", pos=" + itemStartPosOfFile);
                    }
                    if (totalLen <= 0 || headLen <= 0 || contextLen < 0 || type < 0 || term <= 0 || prevLogTerm < 0) {
                        throw new RaftException("bad item. file=" + lf.pathname + ", pos=" + itemStartPosOfFile);
                    }
                }

                this.previousTerm = term;
                this.previousIndex = index;

                if (totalLen < headLen) {
                    log.error("totalLen<headLen. file={}, itemStartPos={}", lf.pathname, itemStartPosOfFile);
                    return false;
                }

                crc32c.reset();
                LogFileQueue.updateCrc(crc32c, buf, startPos + 4, LogFileQueue.ITEM_HEADER_SIZE - 4);
                buf.position(startPos + LogFileQueue.ITEM_HEADER_SIZE);
                readHeader = false;
                currentItemLen = totalLen;
                bodyRestLen = totalLen - headLen;
            } else {
                if (buf.remaining() >= bodyRestLen) {
                    LogFileQueue.updateCrc(crc32c, buf, buf.position(), bodyRestLen);
                    buf.position(buf.position() + bodyRestLen);
                    if (crc == crc32c.getValue()) {
                        itemStartPosOfFile += currentItemLen;
                        if (commitIndexChecked) {
                            idxOps.put(this.previousIndex, lf.startPos + itemStartPosOfFile, totalLen);
                        } else {
                            commitIndexChecked = true;
                        }
                    } else {
                        log.warn("crc32c not match. file={}, itemStartPos={}", lf.pathname, itemStartPosOfFile);
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

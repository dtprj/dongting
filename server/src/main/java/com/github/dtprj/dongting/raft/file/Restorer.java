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
package com.github.dtprj.dongting.raft.file;

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

    private boolean checkCommitIndex;

    private long posOfFile;
    private boolean readHeader = true;
    private int crc = 0;
    private int bodyRestLen = 0;
    private int currentItemLen = 0;

    private long previousIndex = 0;
    private long previousTerm = 0;

    public Restorer(IdxOps idxOps, long commitIndex, long commitIndexPos) {
        this.idxOps = idxOps;
        this.commitIndex = commitIndex;
        this.commitIndexPos = commitIndexPos;
    }

    public long restoreFile(ByteBuffer buffer, LogFile lf) throws IOException {
        if (commitIndexPos >= lf.startPos) {
            // check from commitIndexPos
            posOfFile = commitIndexPos & LogFileQueue.FILE_LEN_MASK;
            if (posOfFile < LogFileQueue.FILE_HEADER_SIZE) {
                throw new RaftException("commitIndexPos is invalid. file=" + lf.pathname + ", pos=" + commitIndexPos);
            }
        } else {
            // check full file
            posOfFile = LogFileQueue.FILE_HEADER_SIZE;
        }
        FileChannel channel = lf.channel;
        channel.position(posOfFile);
        while (channel.position() < channel.size()) {
            buffer.clear();
            while (buffer.hasRemaining() && channel.position() < channel.size()) {
                channel.read(buffer);
            }
            buffer.flip();
            if (restore(buffer, lf)) {
                if (buffer.hasRemaining()) {
                    channel.position(channel.position() - buffer.remaining());
                }
            } else {
                break;
            }
        }
        return posOfFile;
    }

    private boolean restore(ByteBuffer buf, LogFile lf) throws IOException {
        while (buf.hasRemaining()) {
            if (readHeader) {
                if (buf.remaining() < LogFileQueue.ITEM_HEADER_SIZE) {
                    return true;
                }
                int startPos = buf.position();
                crc = buf.getInt();
                int totalLen = buf.getInt();
                short headLen = buf.getShort();
                buf.get();//type
                int term = buf.getInt();
                int prevLogTerm = buf.getInt();
                long index = buf.getLong();
                if (totalLen < headLen) {
                    log.error("totalLen<headLen. file={}, itemStartPos={}", lf.pathname, posOfFile);
                    return false;
                }
                if (checkCommitIndex) {
                    if (this.previousTerm != prevLogTerm) {
                        log.warn("prevLogTerm not match. file={}, itemStartPos={}, {}!={}",
                                lf.pathname, posOfFile, this.previousTerm, prevLogTerm);
                        return false;
                    }
                    if (this.previousIndex + 1 != index) {
                        log.warn("index not match. file={}, itemStartPos={}, {}!={}",
                                lf.pathname, posOfFile, this.previousIndex + 1, index);
                        return false;
                    }
                    if (term < this.previousTerm) {
                        log.warn("term not match. file={}, itemStartPos={}, {}<{}",
                                lf.pathname, posOfFile, term, this.previousTerm);
                        return false;
                    }
                } else {
                    if (index != commitIndex) {
                        throw new RaftException("commitIndex not match. file=" + lf.pathname + ", pos=" + posOfFile);
                    }
                }
                this.previousTerm = term;
                this.previousIndex = index;

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
                    int realCrc = (int) crc32c.getValue();
                    if (crc == crc32c.getValue()) {
                        posOfFile += currentItemLen;
                        if (checkCommitIndex) {
                            idxOps.put(this.previousIndex, lf.startPos + posOfFile);
                        } else {
                            checkCommitIndex = true;
                        }
                    } else {
                        if (realCrc == 0) {
                            log.info("Restore complete. The last file is {}, last itemIndex={}, lastTerm={}",
                                    lf.pathname);
                        } else {
                            log.error("crc32c not match. file={}, itemStartPos={}", lf.pathname, posOfFile);
                        }
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

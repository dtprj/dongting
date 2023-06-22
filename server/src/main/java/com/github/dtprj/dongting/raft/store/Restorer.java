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

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.impl.FileUtil;
import com.github.dtprj.dongting.raft.impl.RaftUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class Restorer {
    private static final DtLog log = DtLogs.getLogger(Restorer.class);

    private static final int STATE_ITEM_HEADER = 1;
    private static final int STATE_BIZ_HEADER = 2;
    private static final int STATE_BIZ_BODY = 3;

    private static final int RT_CONTINUE_LOAD = 1;
    private static final int RT_CONTINUE_READ = 2;
    private static final int RT_CURRENT_FILE_FINISHED = 3;
    private static final int RT_RESTORE_FINISHED = 4;


    private final CRC32C crc32c = new CRC32C();
    private final IdxOps idxOps;
    private final FileOps fileOps;
    private final long commitIndex;
    private final long commitIndexPos;

    private boolean commitIndexChecked;

    private long itemStartPosOfFile;
    private int state;
    private final LogHeader header = new LogHeader();
    private int dataReadLength;

    long previousIndex;
    int previousTerm;

    public Restorer(IdxOps idxOps, FileOps fileOps, long commitIndex, long commitIndexPos) {
        this.idxOps = idxOps;
        this.fileOps = fileOps;
        this.commitIndex = commitIndex;
        this.commitIndexPos = commitIndexPos;
    }

    public Pair<Boolean, Long> restoreFile(ByteBuffer buffer, LogFile lf,
                                           Supplier<Boolean> cancelIndicator) throws IOException {
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
            return restoreFile0(buffer, lf, cancelIndicator);
        } else {
            if (header.crcMatch()) {
                return new Pair<>(false, lf.endPos);
            } else {
                throw new RaftException("first item header crc fail: " + lf.file.getPath());
            }
        }
    }

    private Pair<Boolean, Long> restoreFile0(ByteBuffer buffer, LogFile lf,
                                             Supplier<Boolean> cancelIndicator) throws IOException {
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
        state = STATE_ITEM_HEADER;
        while (readPos < fileOps.fileLength()) {
            RaftUtil.checkInitCancel(cancelIndicator);
            int read = FileUtil.syncRead(channel, buffer, readPos);
            if (read <= 0) {
                continue;
            }
            buffer.flip();
            long endPos = readPos + read;
            int result = restore(buffer, lf, endPos);
            switch (result) {
                case RT_CONTINUE_LOAD:
                    LogFileQueue.prepareNextRead(buffer);
                    readPos = endPos;
                    break;
                case RT_CURRENT_FILE_FINISHED:
                    return new Pair<>(false, lf.startPos + itemStartPosOfFile);
                case RT_RESTORE_FINISHED:
                    return new Pair<>(true, lf.startPos + itemStartPosOfFile);
                default:
                    throw new RaftException("error result: " + result);
            }
        }
        return new Pair<>(false, lf.startPos + itemStartPosOfFile);
    }

    private int crcFail(LogFile lf) {
        if (commitIndexChecked) {
            log.info("reach end of file. file={}, pos={}", lf.file.getPath(), itemStartPosOfFile);
            return RT_RESTORE_FINISHED;
        } else {
            throw new RaftException("commit index crc not match. " + commitIndex + "," + commitIndexPos);
        }
    }

    private int restore(ByteBuffer buf, LogFile lf, long lastReadEndPos) throws IOException {
        long fileLen = fileOps.fileLength();
        while (true) {
            int result;
            if (state == STATE_ITEM_HEADER) {
                result = restoreHeader(buf, lf, fileLen, lastReadEndPos);
            } else if (state == STATE_BIZ_HEADER) {
                int dataLen = header.bizHeaderLen;
                result = restoreData(buf, dataLen, lf, STATE_BIZ_BODY);
            } else if (state == STATE_BIZ_BODY) {
                int dataLen = header.bodyLen;
                result = restoreData(buf, dataLen, lf, STATE_BIZ_HEADER);
                if (result == RT_CONTINUE_READ) {
                    if (commitIndexChecked) {
                        idxOps.put(this.previousIndex, lf.startPos + itemStartPosOfFile);
                    } else {
                        commitIndexChecked = true;
                    }
                    itemStartPosOfFile += header.totalLen;
                }
            } else {
                throw new RaftException("error state: " + state);
            }
            if (result == RT_CONTINUE_READ) {
                continue;
            }
            return result;
        }
    }

    private int restoreHeader(ByteBuffer buf, LogFile lf, long fileLen, long lastReadEndPos) {
        if (buf.remaining() < LogHeader.ITEM_HEADER_SIZE) {
            long restLen = fileLen - lastReadEndPos + buf.remaining();
            return restLen >= LogHeader.ITEM_HEADER_SIZE ? RT_CONTINUE_LOAD : RT_CURRENT_FILE_FINISHED;
        }
        header.read(buf);
        if (!header.crcMatch()) {
            return crcFail(lf);
        }
        if (!header.checkHeader(itemStartPosOfFile, fileOps.fileLength())) {
            throwEx("header check fail", lf, itemStartPosOfFile);
        }
        if (commitIndexChecked) {
            if (header.prevLogTerm != previousTerm) {
                throwEx("prevLogTerm not match", lf, itemStartPosOfFile);
            }
            if (this.previousIndex + 1 != header.index) {
                throwEx("index not match", lf, itemStartPosOfFile);
            }
            if (header.term < this.previousTerm) {
                throwEx("term not match", lf, itemStartPosOfFile);
            }
        } else {
            if (header.index != commitIndex) {
                throwEx("commitIndex not match", lf, itemStartPosOfFile);
            }
            if (header.term <= 0 || header.prevLogTerm < 0) {
                throwEx("invalid term", lf, itemStartPosOfFile);
            }
        }

        this.previousTerm = header.term;
        this.previousIndex = header.index;
        changeState(STATE_BIZ_HEADER);
        return RT_CONTINUE_READ;
    }

    private int restoreData(ByteBuffer buf, int dataLen, LogFile lf, int newState) {
        if (dataLen == 0) {
            changeState(newState);
            return RT_CONTINUE_READ;
        }
        int needRead = dataLen - dataReadLength;
        if (needRead > 0 && buf.remaining() > 0) {
            int actualRead = Math.min(needRead, buf.remaining());
            RaftUtil.updateCrc(crc32c, buf, buf.position(), actualRead);
            buf.position(buf.position() + actualRead);
            dataReadLength += actualRead;
        }
        needRead = dataLen - dataReadLength;
        if (needRead == 0 && buf.remaining() >= 4) {
            if (buf.getInt() != crc32c.getValue()) {
                return crcFail(lf);
            }
            changeState(newState);
            return RT_CONTINUE_READ;
        } else {
            return RT_CONTINUE_LOAD;
        }
    }

    private void changeState(int newState) {
        state = newState;
        dataReadLength = 0;
        crc32c.reset();
    }

    private void throwEx(String msg, LogFile lf, long itemStartPosOfFile) {
        throw new RaftException(msg + ". file=" + lf.file.getPath() + ", pos=" + itemStartPosOfFile);
    }


}

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
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftUtil;

import java.nio.ByteBuffer;
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
    private final LogFileQueue logFileQueue;
    private final Supplier<Boolean> stopIndicator;
    private final long restoreIndex;
    private final long restoreIndexPos;
    private final long firstValidPos;

    private boolean restoreIndexChecked;
    private final LogHeader header = new LogHeader();

    private long itemStartPosOfFile;
    private int state;
    private int dataReadLength;

    long previousIndex;
    int previousTerm;

    public Restorer(IdxOps idxOps, LogFileQueue logFileQueue, Supplier<Boolean> stopIndicator, long restoreIndex,
                    long restoreIndexPos, long firstValidPos) {
        this.idxOps = idxOps;
        this.logFileQueue = logFileQueue;
        this.stopIndicator = stopIndicator;
        this.restoreIndex = restoreIndex;
        this.restoreIndexPos = restoreIndexPos;
        this.firstValidPos = firstValidPos;
    }

    /**
     * return value (left): restore complete
     * return value (right): next write pos
     */
    public FiberFrame<Pair<Boolean, Long>> restoreFile(ByteBuffer buffer, LogFile lf) {
        return new RestoreFileFrame(buffer, lf);
    }

    private class RestoreFileFrame extends FiberFrame<Pair<Boolean, Long>> {
        private final ByteBuffer buffer;
        private final LogFile lf;
        private long readPos;

        RestoreFileFrame(ByteBuffer buffer, LogFile lf) {
            this.buffer = buffer;
            this.lf = lf;
        }

        @Override
        public FrameCallResult execute(Void input) {
            buffer.clear();
            buffer.limit(LogHeader.ITEM_HEADER_SIZE);
            long firstItemPos;
            if (firstValidPos > 0 && firstValidPos > lf.startPos && firstValidPos < lf.endPos) {
                if (lf.endPos - firstValidPos < LogHeader.ITEM_HEADER_SIZE) {
                    // after install snapshot, the firstValidPos is too large in file, so this file has no items
                    setResult(new Pair<>(false, lf.endPos));
                    return Fiber.frameReturn();
                }
                firstItemPos = logFileQueue.filePos(firstValidPos);
            } else {
                firstItemPos = 0;
            }
            AsyncIoTask task = new AsyncIoTask(FiberGroup.currentGroup(), lf.channel, stopIndicator, null);
            return task.read(buffer, firstItemPos).awaitOn(this::afterReadFirstItemHeader);
        }

        private FrameCallResult afterReadFirstItemHeader(Void unused) {
            buffer.flip();
            header.read(buffer);
            if (header.crcMatch()) {
                if (!header.isEndMagic()) {
                    lf.firstIndex = header.index;
                    lf.firstTerm = header.term;
                    lf.firstTimestamp = header.timestamp;
                }
            }

            if (lf.endPos > restoreIndexPos) {
                log.info("try restore file {}", lf.file.getPath());
                if (restoreIndexPos >= lf.startPos) {
                    // check from restoreIndex
                    itemStartPosOfFile = logFileQueue.filePos(restoreIndexPos);
                } else {
                    // check full file
                    itemStartPosOfFile = 0;
                }
                readPos = itemStartPosOfFile;
                buffer.clear();
                state = STATE_ITEM_HEADER;
                return loopRestoreFileBlock();
            } else {
                if (header.crcMatch()) {
                    setResult(new Pair<>(false, lf.endPos));
                    return Fiber.frameReturn();
                } else {
                    throw new RaftException("first item header crc fail: " + lf.file.getPath());
                }
            }
        }

        private FrameCallResult loopRestoreFileBlock() {
            if (readPos < logFileQueue.fileLength()) { // loop begin
                RaftUtil.checkStop(stopIndicator);
                AsyncIoTask task = new AsyncIoTask(getFiberGroup(), lf.channel, stopIndicator, null);
                long fileRest = logFileQueue.fileLength() - readPos;
                if (buffer.remaining() > fileRest) {
                    buffer.limit(buffer.position() + (int) fileRest);
                }
                int readBytes = buffer.remaining();
                return task.read(buffer, readPos).awaitOn(unusedVoid -> afterRead(readBytes));
            }
            // loop finished
            if (state == STATE_ITEM_HEADER) {
                setResult(new Pair<>(false, lf.endPos));
                return Fiber.frameReturn();
            } else {
                throw new RaftException("end of file, state=" + state + ", file=" + lf.file.getPath());
            }
        }

        private FrameCallResult afterRead(int readBytes) {
            buffer.flip();
            int result = restore(buffer, lf);
            switch (result) {
                case RT_CONTINUE_LOAD:
                    StoreUtil.prepareNextRead(buffer);
                    readPos += readBytes;
                    break;
                case RT_CURRENT_FILE_FINISHED:
                    setResult(new Pair<>(false, lf.startPos + itemStartPosOfFile));
                    return Fiber.frameReturn();
                case RT_RESTORE_FINISHED:
                    setResult(new Pair<>(true, lf.startPos + itemStartPosOfFile));
                    return Fiber.frameReturn();
                default:
                    throw new RaftException("error result: " + result);
            }
            // loop
            return loopRestoreFileBlock();
        }
    } //end of class RestoreFileFrame

    private int crcFail(LogFile lf) {
        if (restoreIndexChecked) {
            if (header.totalLen == 0) {
                log.info("reach end of file. file={}, pos={}", lf.file.getPath(), itemStartPosOfFile);
            } else {
                log.warn("reach end of file. last write maybe not finished. file={}, pos={}",
                        lf.file.getPath(), itemStartPosOfFile);
            }
            return RT_RESTORE_FINISHED;
        } else {
            throw new RaftException("restore index crc not match. " + restoreIndex + "," + restoreIndexPos);
        }
    }

    private int restore(ByteBuffer buf, LogFile lf) {
        while (true) {
            int result;
            if (state == STATE_ITEM_HEADER) {
                result = restoreHeader(buf, lf);
            } else if (state == STATE_BIZ_HEADER) {
                int dataLen = header.bizHeaderLen;
                result = restoreData(buf, dataLen, lf, STATE_BIZ_BODY);
            } else if (state == STATE_BIZ_BODY) {
                int dataLen = header.bodyLen;
                result = restoreData(buf, dataLen, lf, STATE_ITEM_HEADER);
                if (result == RT_CONTINUE_READ) {
                    if (!restoreIndexChecked) {
                        restoreIndexChecked = true;
                    }
                    idxOps.put(this.previousIndex, lf.startPos + itemStartPosOfFile, true);
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

    private int restoreHeader(ByteBuffer buf, LogFile lf) {
        if (buf.remaining() < LogHeader.ITEM_HEADER_SIZE) {
            return RT_CONTINUE_LOAD;
        }
        header.read(buf);
        if (!header.crcMatch()) {
            return crcFail(lf);
        }
        if (header.isEndMagic()) {
            return RT_CURRENT_FILE_FINISHED;
        }
        if (!header.checkHeader(itemStartPosOfFile, logFileQueue.fileLength())) {
            throwEx("header check fail", lf, itemStartPosOfFile);
        }
        if (restoreIndexChecked) {
            if (header.prevLogTerm != previousTerm) {
                throwEx("prevLogTerm not match", lf, itemStartPosOfFile);
            }
            if (this.previousIndex + 1 != header.index) {
                throwEx("index not match", lf, itemStartPosOfFile);
            }
            if (header.term < this.previousTerm) {
                throwEx("term less than previous term", lf, itemStartPosOfFile);
            }
        } else {
            if (header.index != restoreIndex) {
                throwEx("restoreIndex not match", lf, itemStartPosOfFile);
            }
        }
        if (header.term <= 0 || header.prevLogTerm < 0) {
            throwEx("invalid term", lf, itemStartPosOfFile);
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
            if (buf.getInt() != (int) crc32c.getValue()) {
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

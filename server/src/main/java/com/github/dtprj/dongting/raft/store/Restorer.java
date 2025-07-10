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
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.nio.ByteBuffer;
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
    private final RaftGroupConfigEx groupConfig;
    private final IdxOps idxOps;
    private final LogFileQueue logFileQueue;
    private final long restoreIndex;
    private final long restoreStartPos;
    private final long firstValidPos;

    private boolean restoreIndexChecked;
    private final LogHeader header = new LogHeader();

    private long itemStartPosOfFile;
    private int state;
    private int dataReadLength;

    long previousIndex;
    int previousTerm;

    int restoreCount;

    public Restorer(RaftGroupConfigEx groupConfig, IdxOps idxOps, LogFileQueue logFileQueue, long restoreIndex,
                    long restoreStartPos, long firstValidPos) {
        this.groupConfig = groupConfig;
        this.idxOps = idxOps;
        this.logFileQueue = logFileQueue;
        this.restoreIndex = restoreIndex;
        this.restoreStartPos = restoreStartPos;
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
            AsyncIoTask task = new AsyncIoTask(groupConfig.fiberGroup, lf);
            return task.read(buffer, firstItemPos).await(v -> afterReadFirstItemHeader(firstItemPos));
        }

        private FrameCallResult afterReadFirstItemHeader(long firstItemPos) {
            buffer.flip();
            header.read(buffer);
            if (header.crcMatch()) {
                if (header.isEndMagic()) {
                    log.info("first item is end magic. file={}, pos={}", lf.getFile().getPath(), firstItemPos);
                    setResult(new Pair<>(false, lf.startPos + firstItemPos));
                    return Fiber.frameReturn();
                } else {
                    lf.firstIndex = header.index;
                    lf.firstTerm = header.term;
                    lf.firstTimestamp = header.timestamp;

                    if (restoreStartPos < lf.endPos) {
                        log.info("try restore file {}", lf.getFile().getPath());
                        if (restoreStartPos >= lf.startPos) {
                            // check from restoreIndex
                            itemStartPosOfFile = logFileQueue.filePos(restoreStartPos);
                        } else {
                            // check full file
                            itemStartPosOfFile = 0;
                        }
                        readPos = itemStartPosOfFile;
                        buffer.clear();
                        state = STATE_ITEM_HEADER;
                        return loopRestoreFileBlock();
                    } else {
                        // no need restore
                        setResult(new Pair<>(false, lf.endPos));
                        return Fiber.frameReturn();
                    }
                }
            } else {
                if (restoreIndexChecked || (restoreStartPos == 0 && restoreIndex == 1)) {
                    log.info("file has no valid item: {}", lf.getFile().getPath());
                    setResult(new Pair<>(true, lf.startPos));
                    return Fiber.frameReturn();
                } else {
                    throw new RaftException("first item header crc not match. file=" + lf.getFile().getPath()
                            + ", pos=" + firstItemPos);
                }
            }
        }

        private FrameCallResult loopRestoreFileBlock() {
            if (readPos < logFileQueue.fileLength()) { // loop begin
                RaftUtil.checkStop(getFiberGroup());
                AsyncIoTask task = new AsyncIoTask(groupConfig.fiberGroup, lf);
                long fileRest = logFileQueue.fileLength() - readPos;
                if (buffer.remaining() > fileRest) {
                    buffer.limit(buffer.position() + (int) fileRest);
                }
                int readBytes = buffer.remaining();
                return task.read(buffer, readPos).await(unusedVoid -> afterRead(readBytes));
            }
            // loop finished
            if (state == STATE_ITEM_HEADER) {
                setResult(new Pair<>(false, lf.endPos));
                return Fiber.frameReturn();
            } else {
                throw new RaftException("end of file, state=" + state + ", file=" + lf.getFile().getPath());
            }
        }

        private FrameCallResult afterRead(int readBytes) {
            buffer.flip();
            int result = restore(buffer, lf);
            if (idxOps.needWaitFlush()) {
                return Fiber.call(idxOps.waitFlush(), v -> afterIdxFlush(readBytes, result));
            } else {
                return afterIdxFlush(readBytes, result);
            }
        }

        private FrameCallResult afterIdxFlush(int readBytes, int result) {
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

    private int itemCheckFail(LogFile lf, String reason) {
        if (restoreIndexChecked) {
            if (header.totalLen == 0 && header.headerCrc == 0) {
                log.info("reach end of file. file={}, pos={}", lf.getFile().getPath(), itemStartPosOfFile);
            } else {
                log.warn("reach end of file. last write maybe not finished or truncated. file={}, pos={}, index={}, term={}, reason={}",
                        lf.getFile().getPath(), itemStartPosOfFile, header.index, header.term, reason);
            }
            return RT_RESTORE_FINISHED;
        } else {
            throw new RaftException("item check fail: " + reason + ". restoreIndex="
                    + restoreIndex + ",restoreStartPos" + restoreStartPos);
        }
    }

    private int restore(ByteBuffer buf, LogFile lf) {
        while (true) {
            int result;
            if (state == STATE_ITEM_HEADER) {
                result = restoreHeader(buf, lf);
            } else if (state == STATE_BIZ_HEADER) {
                int dataLen = header.bizHeaderLen;
                result = restoreData(buf, dataLen, lf, STATE_BIZ_BODY, "biz header");
            } else if (state == STATE_BIZ_BODY) {
                int dataLen = header.bodyLen;
                result = restoreData(buf, dataLen, lf, STATE_ITEM_HEADER, "biz body");
                if (result == RT_CONTINUE_READ) {
                    if (!restoreIndexChecked) {
                        restoreIndexChecked = true;
                    }
                    idxOps.put(this.previousIndex, lf.startPos + itemStartPosOfFile);
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
            return itemCheckFail(lf, "header crc not match");
        }
        if (header.isEndMagic()) {
            return RT_CURRENT_FILE_FINISHED;
        }
        if (!header.checkHeader(itemStartPosOfFile, logFileQueue.fileLength())) {
            throwEx("header check fail", lf, itemStartPosOfFile);
        }
        if (restoreIndexChecked) {
            if (header.prevLogTerm != previousTerm) {
                return itemCheckFail(lf, "prevLogTerm not match");
            }
            if (this.previousIndex + 1 != header.index) {
                return itemCheckFail(lf, "index not match");
            }
            if (header.term < this.previousTerm) {
                return itemCheckFail(lf, "term less than previous term");
            }
        } else {
            if (header.index != restoreIndex) {
                throwEx("restoreIndex not match: " + header.index + "," + restoreIndex, lf, itemStartPosOfFile);
            }
        }
        if (header.term <= 0 || header.prevLogTerm < 0) {
            throwEx("invalid term", lf, itemStartPosOfFile);
        }

        changeState(STATE_BIZ_HEADER);
        return RT_CONTINUE_READ;
    }

    private int restoreData(ByteBuffer buf, int dataLen, LogFile lf, int newState, String dataType) {
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
                return itemCheckFail(lf, dataType + "crc not match");
            }
            changeState(newState);
            return RT_CONTINUE_READ;
        } else {
            return RT_CONTINUE_LOAD;
        }
    }

    private void changeState(int newState) {
        if (newState == STATE_ITEM_HEADER) {
            this.previousTerm = header.term;
            this.previousIndex = header.index;
            this.restoreCount++;
        }
        state = newState;
        dataReadLength = 0;
        crc32c.reset();
    }

    private void throwEx(String msg, LogFile lf, long itemStartPosOfFile) {
        throw new RaftException(msg + ". file=" + lf.getFile().getPath() + ", pos=" + itemStartPosOfFile);
    }
}

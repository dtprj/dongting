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
 * Restores a log file by batched mmap scan in io callbacks + fiber-side idx put, packing item
 * metadata into a fixed buffer so a large file of small items does not blow up memory.
 *
 * @author huangli
 */
class Restorer {
    private static final DtLog log = DtLogs.getLogger(Restorer.class);

    // per-item stride in metaBuf: index(8) + pos(8) + timestamp(8) + totalLen(4)
    static final int ITEM_STRIDE = 3 * Long.BYTES + Integer.BYTES;

    private final RaftGroupConfigEx groupConfig;
    private final IdxOps idxOps;
    private final LogFileQueue logFileQueue;
    private final long restoreIndex;
    private final long restoreStartPos;
    private final long firstValidPos;

    // accumulated state across files; read/written on the io thread during parseFile, the cross-thread
    // visibility between files is established by the await future then the next io submit.
    private boolean restoreIndexChecked;
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
    public FiberFrame<Pair<Boolean, Long>> restoreFile(ByteBuffer metaBuf, LogFile lf) {
        return new RestoreFileFrame(metaBuf, lf);
    }

    private class RestoreFileFrame extends FiberFrame<Pair<Boolean, Long>> {
        private final LogFile lf;
        private final ByteBuffer metaBuf;
        private boolean restoreFinished;
        private long nextWritePos;

        // batch-scanning state. firstScan guards the one-time phase1 (read first item header, set lf
        // meta). scanPos is the next item boundary to resume from; fileDone marks the file fully scanned.
        private boolean firstScan = true;
        private long scanPos;
        private boolean fileDone;

        RestoreFileFrame(ByteBuffer metaBuf, LogFile lf) {
            this.metaBuf = metaBuf;
            this.lf = lf;
            this.metaBuf.clear();
        }

        @Override
        public FrameCallResult execute(Void input) {
            // the restore process do not need to maintain readers count since the raft group is not init.
            MmapIoTask task = new MmapIoTask(groupConfig.fiberGroup, lf);
            return task.run(this::parseFile).await(v -> afterScan());
        }

        private FrameCallResult afterScan() {
            metaBuf.flip();
            return putLoop();
        }

        private FrameCallResult putLoop() {
            while (metaBuf.remaining() >= ITEM_STRIDE) {
                long index = metaBuf.getLong();
                long pos = metaBuf.getLong();
                long timestamp = metaBuf.getLong();
                int totalLen = metaBuf.getInt();
                idxOps.put(index, pos, timestamp, totalLen);
                if (idxOps.needWaitFlush()) {
                    return Fiber.call(idxOps.waitFlush(), v -> putLoop());
                }
            }
            if (fileDone) {
                setResult(new Pair<>(restoreFinished, nextWritePos));
                return Fiber.frameReturn();
            }
            metaBuf.clear();
            return Fiber.resume(null, this);
        }

        // runs in the io thread. On the first call it reads the first item header to determine file meta
        // (phase1), then scans a batch of items (phase2). Subsequent calls skip phase1 and resume phase2
        // from scanPos. Each call fills metaBuf with at most metaBuf.capacity() / ITEM_STRIDE items.
        private void parseFile(ByteBuffer mmapBuffer) {
            LogHeader header = new LogHeader();
            CRC32C crc = new CRC32C();
            long fileLength = logFileQueue.fileLength();
            mmapBuffer.limit((int) fileLength);

            if (firstScan) {
                firstScan = false;
                long firstItemPos;
                if (firstValidPos > 0 && firstValidPos > lf.startPos && firstValidPos < lf.endPos) {
                    if (lf.endPos - firstValidPos < LogHeader.ITEM_HEADER_SIZE) {
                        // after install snapshot, the firstValidPos is too large in file, so this file has no items
                        finishFile(false, lf.endPos);
                        return;
                    }
                    firstItemPos = logFileQueue.filePos(firstValidPos);
                } else {
                    firstItemPos = 0;
                }
                mmapBuffer.position((int) firstItemPos);

                if (!header.readAndCheckCrc(crc, mmapBuffer)) {
                    if (restoreIndexChecked || (restoreStartPos == 0 && restoreIndex == 1)) {
                        log.info("file has no valid item: {}", lf.getFile().getPath());
                        finishFile(true, lf.startPos);
                    } else {
                        throw new RaftException("first item header crc not match. file=" + lf.getFile().getPath()
                                + ", pos=" + firstItemPos);
                    }
                    return;
                }
                if (header.isEndMagic()) {
                    log.info("first item is end magic. file={}, pos={}", lf.getFile().getPath(), firstItemPos);
                    finishFile(false, lf.startPos + firstItemPos);
                    return;
                }
                lf.firstIndex = header.index;
                lf.firstTerm = header.term;
                lf.firstTimestamp = header.timestamp;

                if (restoreStartPos >= lf.endPos) {
                    // no need restore
                    finishFile(false, lf.endPos);
                    return;
                }
                log.info("try restore file {}", lf.getFile().getPath());

                scanPos = restoreStartPos >= lf.startPos ? logFileQueue.filePos(restoreStartPos) : 0;
            }

            // ---- phase2: scan a batch of items from scanPos, packing metadata into metaBuf ----
            mmapBuffer.position((int) scanPos);
            while (true) {
                // shouldStop is a non-volatile write-once flag, so a stale read on the io thread at worst
                // delays shutdown detection until this batch completes.
                RaftUtil.checkStop(groupConfig.fiberGroup);
                if (metaBuf.remaining() < ITEM_STRIDE) {
                    return; // metaBuf full; scanPos already at the next item boundary, fileDone stays false
                }
                if (mmapBuffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                    finishFile(false, lf.endPos);
                    return;
                }
                long itemStartPosOfFile = scanPos;
                if (!header.readAndCheckCrc(crc, mmapBuffer)) {
                    failOrFinish(itemStartPosOfFile, header, "header crc not match");
                    return;
                }
                if (header.isEndMagic()) {
                    finishFile(false, lf.startPos + itemStartPosOfFile);
                    return;
                }
                if (!header.checkHeader(itemStartPosOfFile, fileLength)) {
                    throw new RaftException("header check fail. file=" + lf.getFile().getPath()
                            + ", pos=" + itemStartPosOfFile);
                }
                if (restoreIndexChecked) {
                    String reason = null;
                    if (header.prevLogTerm != previousTerm) {
                        reason = "prevLogTerm not match";
                    } else if (previousIndex + 1 != header.index) {
                        reason = "index not match";
                    } else if (header.term < previousTerm) {
                        reason = "term less than previous term";
                    }
                    if (reason != null) {
                        failOrFinish(itemStartPosOfFile, header, reason);
                        return;
                    }
                } else {
                    if (header.index != restoreIndex) {
                        throw new RaftException("restoreIndex not match: " + header.index + "," + restoreIndex
                                + ". file=" + lf.getFile().getPath() + ", pos=" + itemStartPosOfFile);
                    }
                }
                if (header.term <= 0 || header.prevLogTerm < 0) {
                    throw new RaftException("invalid term. file=" + lf.getFile().getPath()
                            + ", pos=" + itemStartPosOfFile);
                }
                // checkHeader guarantees the whole item is within the file, so data is always complete here
                if (!checkData(mmapBuffer, header.bizHeaderLen, crc)) {
                    failOrFinish(itemStartPosOfFile, header, "biz header crc not match");
                    return;
                }
                if (!checkData(mmapBuffer, header.bodyLen, crc)) {
                    failOrFinish(itemStartPosOfFile, header, "biz body crc not match");
                    return;
                }

                restoreIndexChecked = true;
                previousIndex = header.index;
                previousTerm = header.term;
                restoreCount++;
                metaBuf.putLong(header.index);
                metaBuf.putLong(lf.startPos + itemStartPosOfFile);
                metaBuf.putLong(header.timestamp);
                metaBuf.putInt(header.totalLen);
                scanPos = itemStartPosOfFile + header.totalLen;
            }
        }

        // marks the file fully scanned and sets the restore result for the caller.
        private void finishFile(boolean restoreFinished, long nextWritePos) {
            this.fileDone = true;
            this.restoreFinished = restoreFinished;
            this.nextWritePos = nextWritePos;
        }

        // on item check failure: gracefully finish if we already verified restoreIndex (at least one
        // item passed), otherwise the first item is corrupt and restore must fail hard.
        private void failOrFinish(long itemStartPosOfFile, LogHeader header, String reason) {
            if (restoreIndexChecked) {
                if (header.totalLen == 0 && header.term == 0 && header.timestamp == 0 && header.index == 0) {
                    log.info("reach end of file. file={}, pos={}", lf.getFile().getPath(), itemStartPosOfFile);
                } else {
                    log.warn("reach end of file. last write maybe not finished or truncated. file={}, pos={}, index={}, term={}, reason={}",
                            lf.getFile().getPath(), itemStartPosOfFile, header.index, header.term, reason);
                }
                finishFile(true, lf.startPos + itemStartPosOfFile);
            } else {
                throw new RaftException("item check fail: " + reason + ". restoreIndex="
                        + restoreIndex + ", restoreStartPos=" + restoreStartPos
                        + ", file=" + lf.getFile().getPath() + ", pos=" + itemStartPosOfFile);
            }
        }
    }

    private static boolean checkData(ByteBuffer buf, int dataLen, CRC32C crc) {
        if (dataLen == 0) {
            return true;
        }
        crc.reset();
        int start = buf.position();
        RaftUtil.updateCrc(crc, buf, start, dataLen);
        buf.position(start + dataLen);
        return buf.getInt() == (int) crc.getValue();
    }
}

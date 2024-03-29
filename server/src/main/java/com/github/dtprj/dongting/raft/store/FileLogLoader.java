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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftCancelException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class FileLogLoader implements RaftLog.LogIterator {

    private static final int STATE_ITEM_HEADER = 1;
    private static final int STATE_BIZ_HEADER = 2;
    private static final int STATE_BIZ_BODY = 3;

    private final IdxOps idxFiles;
    private final LogFileQueue logFiles;
    private final ByteBufferPool heapPool;
    private final RaftGroupConfigEx groupConfig;
    private final ByteBuffer readBuffer;
    private final TailCache tailCache;

    private final Supplier<Boolean> cancelIndicator;
    private final CRC32C crc32c = new CRC32C();
    private final LogHeader header = new LogHeader();

    private boolean error;
    private boolean close;

    // status cross next calls
    private long nextIndex;
    private long nextPos;
    private long bufferStartPos;
    private long bufferEndPos;
    private LogFile logFile;

    FileLogLoader(IdxOps idxFiles, LogFileQueue logFiles, RaftGroupConfigEx groupConfig,
                  Supplier<Boolean> cancelIndicator) {
        this(idxFiles, logFiles, groupConfig, cancelIndicator, 256 * 1024);
    }

    FileLogLoader(IdxOps idxFiles, LogFileQueue logFiles, RaftGroupConfigEx groupConfig,
                  Supplier<Boolean> cancelIndicator, int readBufferSize) {
        this.idxFiles = idxFiles;
        this.logFiles = logFiles;
        this.groupConfig = groupConfig;
        this.heapPool = groupConfig.getHeapPool().getPool();
        this.cancelIndicator = cancelIndicator;
        this.tailCache = ((RaftStatusImpl) groupConfig.getRaftStatus()).getTailCache();

        this.readBuffer = groupConfig.getDirectPool().borrow(readBufferSize);
        reset();
    }

    private void reset() {
        nextIndex = -1;
        nextPos = -1;
        bufferStartPos = -1;
        bufferEndPos = -1;
        logFile = null;

        readBuffer.clear();
        readBuffer.limit(0);
    }

    @Override
    public FiberFrame<List<LogItem>> next(long index, int limit, int bytesLimit) {
        if (error || close) {
            BugLog.getLog().error("iterator state error: {},{}", error, close);
            throw new RaftException("iterator state error");
        }
        if (nextIndex != -1 && index != nextIndex) {
            throw new RaftException("index not match: " + index + "," + nextIndex);
        }
        return new NextFrame(index, limit, bytesLimit);
    }

    private class NextFrame extends FiberFrame<List<LogItem>> {
        private static final int RESULT_CONTINUE_PARSE = 11;
        private static final int RESULT_FINISH = 12;
        private static final int RESULT_NEED_LOAD = 13;

        // status of single next call
        private final long startIndex;
        private final int limit;
        private final int bytesLimit;

        private int readBytes;
        private final List<LogItem> result = new LinkedList<>();
        private int state = STATE_ITEM_HEADER;
        private LogItem item;
        private long itemStartPos;

        NextFrame(long startIndex, int limit, int bytesLimit) {
            this.startIndex = startIndex;
            this.limit = limit;
            this.bytesLimit = bytesLimit;
        }

        @Override
        protected FrameCallResult handle(Throwable ex) throws Throwable {
            error = true;
            throw ex;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (nextIndex == -1) {
                return idxFiles.loadLogPos(startIndex, this::afterStartIndexPosLoad);
            } else {
                if (readBuffer.hasRemaining()) {
                    return parseContent();
                } else {
                    readBuffer.clear();
                    nextPos = bufferEndPos;
                    return loadLogFromStore();
                }
            }
        }

        private FrameCallResult afterStartIndexPosLoad(Long startIndexPos) {
            nextPos = startIndexPos;
            nextIndex = startIndex;
            readBuffer.clear();
            return loadLogFromStore();
        }

        private FrameCallResult parseContent() {
            ByteBuffer buf = readBuffer;
            while (true) {
                int r = doParse(buf);
                if (r == RESULT_FINISH) {
                    setResult(result);
                    return Fiber.frameReturn();
                } else if (r == RESULT_NEED_LOAD) {
                    return loadLogFromStore();
                } else if (r != RESULT_CONTINUE_PARSE) {
                    throw new RaftException("error result:" + r);
                }
            }
        }

        private int doParse(ByteBuffer buf) {
            int s = state;
            if (s == STATE_ITEM_HEADER) {
                return processHeader(buf);
            } else if (s == STATE_BIZ_HEADER) {
                return extractBizHeader(buf);
            } else if (s == STATE_BIZ_BODY) {
                return extractBizBody(buf);
            } else {
                throw new RaftException("error state:" + state);
            }
        }


        private FrameCallResult loadLogFromStore() {
            long pos = nextPos;
            logFile = logFiles.getLogFile(pos);
            long rest = logFile.endPos - pos;
            long fileStartPos = logFiles.filePos(pos);
            ByteBuffer buf = readBuffer;
            if (fileStartPos == 0 && buf.position() > 0) {
                RaftException e = new RaftException("readBuffer not empty when load from file start position");
                BugLog.log(e);
                throw e;
            }
            if (rest < buf.remaining()) {
                // not overflow
                buf.limit((int) (buf.position() + rest));
            }
            bufferStartPos = pos - buf.position();
            bufferEndPos = pos + buf.remaining();
            AsyncIoTask t = new AsyncIoTask(getFiberGroup(), logFile);
            return Fiber.call(t.lockRead(buf, fileStartPos), this::resumeAfterLoad);
        }

        private FrameCallResult resumeAfterLoad(Void v) {
            if (cancelIndicator != null && cancelIndicator.get()) {
                throw new RaftCancelException("canceled");
            } else {
                readBuffer.flip();
                // loop
                return parseContent();
            }
        }

        private void discardBufferAndLoadNextFile(ByteBuffer buf) {
            buf.clear();
            nextPos = logFiles.nextFilePos(bufferStartPos);
        }

        private int processHeader(ByteBuffer buf) {
            if (buf.remaining() >= LogHeader.ITEM_HEADER_SIZE) {
                if (!extractHeader(buf)) {
                    // reached end of file
                    discardBufferAndLoadNextFile(buf);
                    return RESULT_NEED_LOAD;
                }
                crc32c.reset();
                state = STATE_BIZ_HEADER;
                if (!result.isEmpty() && header.bodyLen + readBytes >= bytesLimit) {
                    buf.position(buf.position() - LogHeader.ITEM_HEADER_SIZE);
                    finish();
                    return RESULT_FINISH;
                } else {
                    return RESULT_CONTINUE_PARSE;
                }
            } else {
                long rest = logFile.endPos - bufferEndPos + buf.remaining();
                if (rest < LogHeader.ITEM_HEADER_SIZE) {
                    // reached end of file
                    discardBufferAndLoadNextFile(buf);
                } else {
                    StoreUtil.prepareNextRead(buf);
                    nextPos = bufferEndPos;
                }
                return RESULT_NEED_LOAD;
            }
        }

        private boolean extractHeader(ByteBuffer readBuffer) {
            LogHeader h = header;
            itemStartPos = bufferStartPos + readBuffer.position();
            h.read(readBuffer);
            if (h.isEndMagic()) {
                return false;
            }
            if (!h.crcMatch()) {
                throw new ChecksumException("header crc not match: index=" + (nextIndex + result.size())
                        + ",pos=" + itemStartPos + ",len=" + h.totalLen);
            }

            int bodyLen = h.bodyLen;
            if (!h.checkHeader(logFiles.filePos(itemStartPos), logFiles.fileLength())) {
                throw new RaftException("header check fail: index=" + (nextIndex + result.size()) + ",pos=" + itemStartPos);
            }

            LogItem li = new LogItem(heapPool);
            this.item = li;
            h.copy(li);


            int bizHeaderLen = h.bizHeaderLen;
            li.setActualHeaderSize(bizHeaderLen);
            if (bizHeaderLen > 0) {
                li.setHeaderBuffer(heapPool.borrow(bizHeaderLen));
            }

            li.setActualBodySize(bodyLen);
            if (bodyLen > 0) {
                li.setBodyBuffer(heapPool.borrow(bodyLen));
            }
            return true;
        }

        private int extractBizHeader(ByteBuffer buf) {
            int bizHeaderLen = header.bizHeaderLen;
            if (bizHeaderLen == 0) {
                state = STATE_BIZ_BODY;
                return RESULT_CONTINUE_PARSE;
            }
            ByteBuffer destBuf = item.getHeaderBuffer();
            boolean readFinish = readData(buf, bizHeaderLen, destBuf);
            if (readFinish) {
                crc32c.reset();
                state = STATE_BIZ_BODY;
                return RESULT_CONTINUE_PARSE;
            } else {
                StoreUtil.prepareNextRead(buf);
                nextPos = bufferEndPos;
                return RESULT_NEED_LOAD;
            }
        }

        private boolean readData(ByteBuffer buf, int dataLen, ByteBuffer destBuf) {
            int read = destBuf.position();
            int needRead = dataLen - read;
            if (needRead > 0 && buf.remaining() > 0) {
                int actualRead = Math.min(needRead, buf.remaining());
                RaftUtil.updateCrc(crc32c, buf, buf.position(), actualRead);
                buf.get(destBuf.array(), read, actualRead);
                destBuf.position(read + actualRead);
            }
            needRead = dataLen - destBuf.position();
            if (needRead == 0 && buf.remaining() >= 4) {
                destBuf.flip();
                int crc = (int) crc32c.getValue();
                if (crc != buf.getInt()) {
                    throw new ChecksumException("crc32c not match: index=" + header.index + ",pos=" + itemStartPos + ",len=" + dataLen);
                }
                return true;
            } else {
                return false;
            }
        }

        private int extractBizBody(ByteBuffer buf) {
            int bodyLen = header.bodyLen;
            if (bodyLen == 0) {
                result.add(item);
                item = null;
                state = STATE_ITEM_HEADER;
                return checkItemLimit();
            }
            ByteBuffer destBuf = item.getBodyBuffer();
            boolean readFinish = readData(buf, bodyLen, destBuf);
            if (readFinish) {
                result.add(item);
                readBytes += bodyLen;
                item = null;
                state = STATE_ITEM_HEADER;
                return checkItemLimit();
            } else {
                StoreUtil.prepareNextRead(buf);
                nextPos = bufferEndPos;
                return RESULT_NEED_LOAD;
            }
        }

        private int checkItemLimit() {
            if (result.size() >= limit) {
                finish();
                return RESULT_FINISH;
            } else {
                long index = nextIndex + result.size();
                RaftTask rt = tailCache.get(index);
                if (rt == null || rt.getInput().isReadOnly()) {
                    return RESULT_CONTINUE_PARSE;
                } else {
                    // rest items in tail cache
                    FileLogLoader.this.reset();
                    return RESULT_FINISH;
                }
            }
        }

        private void finish() {
            nextIndex += result.size();
        }
    }

    @Override
    public void close() {
        if (!close) {
            groupConfig.getDirectPool().release(readBuffer);
        }
        close = true;
    }
}

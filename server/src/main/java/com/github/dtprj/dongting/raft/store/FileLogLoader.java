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
import com.github.dtprj.dongting.codec.ByteArrayEncoder;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.common.DtThread;
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
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

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
    private final RaftGroupConfigEx groupConfig;
    private final ByteBuffer readBuffer;
    private final TailCache tailCache;
    private final ByteBufferPool directPool;

    private final Supplier<Boolean> cancelIndicator;
    private final CRC32C crc32c = new CRC32C();
    private final LogHeader header = new LogHeader();
    private final RaftCodecFactory codecFactory;
    private final DecodeContext decodeContext;

    private boolean error;
    private boolean close;

    // status cross next calls
    private long nextIndex;
    private long nextPos;
    private long bufferStartPos;
    private long bufferEndPos;
    private LogFile logFile;

    FileLogLoader(IdxOps idxFiles, LogFileQueue logFiles, RaftGroupConfigEx groupConfig, RaftCodecFactory codecFactory,
                  Supplier<Boolean> cancelIndicator) {
        this(idxFiles, logFiles, groupConfig, codecFactory, cancelIndicator, 256 * 1024);
    }

    FileLogLoader(IdxOps idxFiles, LogFileQueue logFiles, RaftGroupConfigEx groupConfig,
                  RaftCodecFactory codecFactory, Supplier<Boolean> cancelIndicator, int readBufferSize) {
        this.idxFiles = idxFiles;
        this.logFiles = logFiles;
        this.groupConfig = groupConfig;
        this.codecFactory = codecFactory;
        this.cancelIndicator = cancelIndicator;
        this.tailCache = ((RaftStatusImpl) groupConfig.getRaftStatus()).getTailCache();

        DtThread t = groupConfig.getFiberGroup().getThread();
        this.directPool = t.getDirectPool();
        this.readBuffer = directPool.borrow(readBufferSize);
        this.decodeContext = new DecodeContext();
        decodeContext.setHeapPool(t.getHeapPool());
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

        private int totalReadBytes;
        private int currentReadBytes;
        private Decoder<? extends Encodable> currentDecoder;
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
        protected FrameCallResult doFinally() {
            resetDecoder();
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (nextIndex == -1) {
                return Fiber.call(idxFiles.loadLogPos(startIndex), this::afterStartIndexPosLoad);
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
            while (true) {
                int r = doParse(readBuffer);
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
            if (logFile.isDeleted()) {
                throw new RaftException("file " + logFile.getFile().getName() + " is deleted");
            }
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
            AsyncIoTask t = new AsyncIoTask(groupConfig.getFiberGroup(), logFile);
            return t.read(buf, fileStartPos).await(this::resumeAfterLoad);
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
                if (!result.isEmpty() && header.bodyLen + totalReadBytes >= bytesLimit) {
                    buf.position(buf.position() - LogHeader.ITEM_HEADER_SIZE);
                    finishRead();
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

            LogItem li = new LogItem();
            this.item = li;
            h.copy(li);

            li.setActualHeaderSize(h.bizHeaderLen);
            li.setActualBodySize(bodyLen);

            return true;
        }

        private int extractBizHeader(ByteBuffer buf) {
            int bizHeaderLen = header.bizHeaderLen;
            if (bizHeaderLen == 0) {
                state = STATE_BIZ_BODY;
                return RESULT_CONTINUE_PARSE;
            }
            boolean readFinish = readData(buf, bizHeaderLen, true);
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

        private boolean readData(ByteBuffer buf, int dataLen, boolean isHeader) {
            if (dataLen - currentReadBytes > 0 && buf.remaining() > 0) {
                int oldPos = buf.position();
                if (currentDecoder == null) {
                    if (header.type == LogItem.TYPE_NORMAL) {
                        currentDecoder = isHeader ? codecFactory.createHeaderDecoder(header.bizType)
                                : codecFactory.createBodyDecoder(header.bizType);
                    } else {
                        currentDecoder = ByteArrayEncoder.DECODER;
                    }
                }
                Encodable result = currentDecoder.decode(decodeContext, buf, dataLen, currentReadBytes);
                if (isHeader) {
                    item.setHeader(result);
                } else {
                    item.setBody(result);
                }
                int read = buf.position() - oldPos;
                if (read > 0) {
                    RaftUtil.updateCrc(crc32c, buf, oldPos, read);
                }
                currentReadBytes += read;
            }
            if (dataLen - currentReadBytes <= 0 && buf.remaining() >= 4) {
                totalReadBytes += currentReadBytes;
                currentReadBytes = 0;
                resetDecoder();
                int crc = (int) crc32c.getValue();
                if (crc != buf.getInt()) {
                    throw new ChecksumException("crc32c not match: index=" + header.index + ",pos="
                            + itemStartPos + ",len=" + dataLen);
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
            boolean readFinish = readData(buf, bodyLen, false);
            if (readFinish) {
                result.add(item);
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
                finishRead();
                return RESULT_FINISH;
            } else {
                long index = nextIndex + result.size();
                RaftTask rt = tailCache.get(index);
                if (rt == null) {
                    return RESULT_CONTINUE_PARSE;
                } else {
                    // rest items in tail cache
                    FileLogLoader.this.reset();
                    return RESULT_FINISH;
                }
            }
        }

        private void finishRead() {
            nextIndex += result.size();
        }

        private void resetDecoder() {
            if (currentDecoder != null) {
                try {
                    currentDecoder.finish(decodeContext);
                    decodeContext.reset();
                } finally {
                    currentDecoder = null;
                }
            }
        }
    }

    @Override
    public void close() {
        if (!close) {
            directPool.release(readBuffer);
        }
        close = true;
    }
}

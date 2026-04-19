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
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.RefBufferDecoderCallback;
import com.github.dtprj.dongting.fiber.DispatcherThread;
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
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
    private ByteBuffer readBuffer;
    private boolean loading;
    private final TailCache tailCache;
    private final ByteBufferPool directPool;

    private final Supplier<Boolean> cancelIndicator;
    private final CRC32C crc32c = new CRC32C();
    private final LogHeader header = new LogHeader();
    private final RaftCodecFactory codecFactory;
    private final DecodeContext decodeContext;
    private final Decoder decoder;
    private final DecoderCallback<RefBuffer> refBufferCallback = new RefBufferDecoderCallback();

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
        this.tailCache = ((RaftStatusImpl) groupConfig.raftStatus).tailCache;

        DispatcherThread t = groupConfig.fiberGroup.dispatcher.thread;
        this.directPool = t.directPool;
        this.readBuffer = directPool.borrow(readBufferSize);
        this.decodeContext = DecodeContext.factory.apply(t.heapPool, t.threadLocalBuffer);
        this.decoder = new Decoder();
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
    public FiberFrame<List<RaftTask>> next(long index, int limit, int bytesLimit) {
        if (error || close || loading) {
            BugLog.log("iterator state error: {},{},{}", error, close, loading);
            throw new RaftException("iterator state error");
        }
        if (nextIndex != -1 && index != nextIndex) {
            throw new RaftException("index not match: " + index + "," + nextIndex);
        }
        return new NextFrame(index, limit, bytesLimit);
    }

    private class NextFrame extends FiberFrame<List<RaftTask>> {
        private static final int RESULT_CONTINUE_PARSE = 11;
        private static final int RESULT_FINISH = 12;
        private static final int RESULT_NEED_LOAD = 13;

        // status of single next call
        private final long startIndex;
        private final int limit;
        private final int bytesLimit;

        private int totalReadBytes;
        private int currentReadBytes;
        private final List<RaftTask> result = new LinkedList<>();
        private int state = STATE_ITEM_HEADER;
        private long itemStartPos;

        private RefBuffer bizHeader;
        private int bizHeaderCrc;

        private RefBuffer bizBody;
        private int bizBodyCrc;

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
            decodeContext.reset(decoder);
            loading = false;
            releaseIfNecessary();
            return Fiber.frameReturn();
        }

        @Override
        public FrameCallResult execute(Void input) {
            loading = true;
            if (nextIndex == -1) {
                return Fiber.call(idxFiles.loadLogPos(startIndex), this::resumeAfterFirstPosLoad);
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

        private FrameCallResult resumeAfterFirstPosLoad(Long startIndexPos) {
            if (cancelIndicator != null && cancelIndicator.get()) {
                throw new RaftCancelException("canceled");
            }
            nextPos = startIndexPos;
            nextIndex = startIndex;
            readBuffer.clear();
            return loadLogFromStore();
        }

        private FrameCallResult parseContent() {
            while (true) {
                int r;
                int s = state;
                if (s == STATE_ITEM_HEADER) {
                    r = processHeader(readBuffer);
                } else if (s == STATE_BIZ_HEADER) {
                    r = extractBizHeader(readBuffer);
                } else if (s == STATE_BIZ_BODY) {
                    r = extractBizBody(readBuffer);
                } else {
                    throw new RaftException("error state:" + state);
                }
                if (r == RESULT_FINISH) {
                    setResult(new ArrayList<>(result));
                    return Fiber.frameReturn();
                } else if (r == RESULT_NEED_LOAD) {
                    return loadLogFromStore();
                } else if (r != RESULT_CONTINUE_PARSE) {
                    throw new RaftException("error result:" + r);
                }
            }
        }

        private FrameCallResult loadLogFromStore() {
            long pos = nextPos;
            logFile = logFiles.getLogFile(pos);
            if (logFile == null) {
                throw new RaftException("log file not found for pos: " + pos);
            }
            if (logFile.isDeleted()) {
                throw new RaftException("file " + logFile.getFile().getName() + " is deleted");
            }
            long fileStartPos = logFiles.filePos(pos);
            ByteBuffer buf = readBuffer;
            if (fileStartPos == 0 && buf.position() > 0) {
                RaftException e = new RaftException("readBuffer not empty when load from file start position");
                BugLog.log(e);
                throw e;
            }
            int rest = (int) (logFile.endPos - pos);
            if (rest < buf.remaining()) {
                buf.limit(buf.position() + rest);
            }
            bufferStartPos = pos - buf.position();
            bufferEndPos = pos + buf.remaining();
            AsyncIoTask t = new AsyncIoTask(groupConfig.fiberGroup, logFile);
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
                if (!result.isEmpty() && header.bodyLen + totalReadBytes > bytesLimit) {
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
            if (!h.readAndCheckCrc(crc32c, readBuffer)) {
                throw new ChecksumException("header crc not match: index=" + (nextIndex + result.size())
                        + ",pos=" + itemStartPos);
            }
            if (h.isEndMagic()) {
                return false;
            }

            if (!h.checkHeader(logFiles.filePos(itemStartPos), logFiles.fileLength())) {
                throw new RaftException("header check fail: index=" + (nextIndex + result.size()) + ",pos=" + itemStartPos);
            }

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
                if (currentReadBytes == 0) {
                    decoder.prepareNext(decodeContext, refBufferCallback);
                }
                RefBuffer result = (RefBuffer) decoder.decode(buf, dataLen, currentReadBytes);
                int read = buf.position() - oldPos;
                if (read > 0) {
                    RaftUtil.updateCrc(crc32c, buf, oldPos, read);
                }
                currentReadBytes += read;
                if (currentReadBytes >= dataLen) {
                    if (isHeader) {
                        bizHeader = result;
                    } else {
                        bizBody = result;
                    }
                }
            }
            if (dataLen - currentReadBytes <= 0 && buf.remaining() >= 4) {
                totalReadBytes += currentReadBytes;
                currentReadBytes = 0;
                int crc = (int) crc32c.getValue();
                if (crc != buf.getInt()) {
                    throw new ChecksumException("crc32c not match: index=" + header.index + ",pos="
                            + itemStartPos + ",len=" + dataLen);
                }
                if (isHeader) {
                    bizHeaderCrc = crc;
                } else {
                    bizBodyCrc = crc;
                }
                return true;
            } else {
                return false;
            }
        }

        private void add() {
            RaftReqData reqData = new RaftReqData(bizHeader, bizHeaderCrc, bizBody, bizBodyCrc);
            Object decodeBizHeader = decodeData(header.type, bizHeader, true);
            Object decodeBizBody = decodeData(header.type, bizBody, false);
            RaftTask rt = new RaftTask(header.type, header.term, header.prevLogTerm,
                    header.index, header.timestamp, header.bizType, reqData,
                    decodeBizHeader, decodeBizBody, header.type == LogHeader.TYPE_LOG_READ);

            // nanos can't persist, use wallClockMillis, so has week dependence on system clock.
            // this method only used to load logs that not apply after restart.
            long costTimeMillis = groupConfig.ts.wallClockMillis - header.timestamp;
            if (costTimeMillis < 0) {
                costTimeMillis = 0;
            }
            long localCreateNanos = groupConfig.ts.nanoTime - costTimeMillis * 1_000_000L;
            rt.init(localCreateNanos);

            result.add(rt);

            bizHeaderCrc = 0;
            bizBodyCrc = 0;

            bizHeader = null;
            bizBody = null;
        }

        private Object decodeData(int type, RefBuffer data, boolean isHeader) {
            if (data == null) {
                return null;
            }
            ByteBuffer buf = data.getBuffer();
            int oldPos = buf.position();
            try {
                if (type != LogHeader.TYPE_NORMAL) {
                    byte[] b = new byte[buf.remaining()];
                    buf.get(b);
                    return b;
                }
                DecoderCallback<?> c = isHeader ? codecFactory.createHeaderCallback(header.bizType, decodeContext) :
                        codecFactory.createBodyCallback(header.bizType, decodeContext);
                int len = isHeader ? header.bizHeaderLen : header.bodyLen;
                decoder.prepareNext(decodeContext, c);
                return decoder.decode(data.getBuffer(), len, 0);
            } finally {
                buf.position(oldPos);
            }
        }

        private int extractBizBody(ByteBuffer buf) {
            int bodyLen = header.bodyLen;
            if (bodyLen == 0) {
                add();
                state = STATE_ITEM_HEADER;
                return checkItemLimit();
            }
            boolean readFinish = readData(buf, bodyLen, false);
            if (readFinish) {
                add();
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
    }

    @Override
    public void close() {
        if (!close) {
            close = true;
            releaseIfNecessary();
        }
    }

    private void releaseIfNecessary() {
        if (close && !loading) {
            directPool.release(readBuffer);
            readBuffer = null;
        }
    }
}

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
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftExecutor;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.impl.StoppedException;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
    private final FileOps logFiles;
    private final RaftExecutor raftExecutor;
    private final ByteBufferPool heapPool;
    private final RaftGroupConfigEx groupConfig;
    private final ByteBuffer readBuffer;

    private final Supplier<Boolean> cancelIndicator;
    private final CRC32C crc32c = new CRC32C();
    private final LogHeader header = new LogHeader();

    private long nextIndex = -1;
    private long nextPos = -1;
    private long bufferStartPos = -1;
    private long bufferEndPos = -1;
    private long itemStartPos = -1;

    private LogFile logFile;

    private boolean error;
    private boolean close;

    private int readBytes;
    private int limit;
    private int bytesLimit;
    private List<LogItem> result;
    private CompletableFuture<List<LogItem>> future;
    private int state;
    private LogItem item;

    FileLogLoader(IdxOps idxFiles, FileOps logFiles, RaftGroupConfigEx groupConfig,
                  Supplier<Boolean> cancelIndicator) {
        this(idxFiles, logFiles, groupConfig, cancelIndicator, 1024 * 1024);
    }

    FileLogLoader(IdxOps idxFiles, FileOps logFiles, RaftGroupConfigEx groupConfig,
                  Supplier<Boolean> cancelIndicator, int readBufferSize) {
        this.idxFiles = idxFiles;
        this.logFiles = logFiles;
        this.raftExecutor = (RaftExecutor) groupConfig.getRaftExecutor();
        this.readBuffer = groupConfig.getDirectPool().borrow(readBufferSize);
        this.groupConfig = groupConfig;
        this.heapPool = groupConfig.getHeapPool().getPool();
        this.readBuffer.limit(0);
        this.cancelIndicator = cancelIndicator;
    }

    @Override
    public CompletableFuture<List<LogItem>> next(long index, int limit, int bytesLimit) {
        try {
            if (error || future != null || close) {
                BugLog.getLog().error("iterator state error: {},{},{}", error, future, close);
                future = null;
                return CompletableFuture.failedFuture(new RaftException("iterator state error"));
            }
            if (nextIndex == -1) {
                // used occasionally when follower not catch up to leader
                // TODO make async later
                nextPos = idxFiles.loadLogPos(index).get();
                nextIndex = index;
            } else {
                if (nextIndex != index) {
                    return CompletableFuture.failedFuture(new RaftException("nextIndex!=index"));
                }
            }

            this.result = new LinkedList<>();
            CompletableFuture<List<LogItem>> f = new CompletableFuture<>();
            this.future = f;
            this.item = null;
            this.readBytes = 0;
            this.limit = limit;
            this.state = STATE_ITEM_HEADER;
            this.bytesLimit = bytesLimit;

            if (readBuffer.hasRemaining()) {
                parseContent();
            } else {
                readBuffer.clear();
                loadLogFromStore(nextPos);
            }
            return f;
        } catch (Throwable e) {
            error = true;
            future = null;
            return CompletableFuture.failedFuture(e);
        }
    }

    private void finish(Throwable ex) {
        error = true;
        future.completeExceptionally(ex);
        future = null;
    }

    private void finishWithCancel() {
        error = true;
        future.cancel(false);
        future = null;
    }

    private void finish(List<LogItem> result, long newPos) {
        future.complete(result);
        nextIndex += result.size();
        nextPos = newPos;
        future = null;
    }

    private void parseContent() {
        ByteBuffer buf = readBuffer;
        while (true) {
            switch (state) {
                case STATE_ITEM_HEADER:
                    if (processHeader(buf)) {
                        continue;
                    } else {
                        return;
                    }
                case STATE_BIZ_HEADER:
                    if (extractBizHeader(buf)) {
                        continue;
                    } else {
                        return;
                    }
                case STATE_BIZ_BODY:
                    if (extractBizBody(buf)) {
                        continue;
                    } else {
                        return;
                    }
                default:
                    throw new RaftException("error state:" + state);
            }
        }
    }

    private void loadLogFromStore(long pos) {
        logFile = logFiles.getLogFile(pos);
        long rest = logFile.endPos - pos;
        long fileStartPos = logFiles.filePos(pos);
        ByteBuffer readBuffer = this.readBuffer;
        if (fileStartPos == 0 && readBuffer.position() > 0) {
            RaftException e = new RaftException("readBuffer not empty when load from file start position");
            BugLog.log(e);
            finish(e);
            return;
        }
        if (rest < readBuffer.remaining()) {
            // not overflow
            readBuffer.limit((int) (readBuffer.position() + rest));
        }
        bufferStartPos = pos - readBuffer.position();
        bufferEndPos = pos + readBuffer.remaining();
        AsyncIoTask t = new AsyncIoTask(logFile.channel, groupConfig.getStopIndicator(), cancelIndicator);
        logFile.use++;
        t.read(readBuffer, fileStartPos).whenCompleteAsync((v, ex) -> resumeAfterLoad(ex), raftExecutor);
    }

    private void resumeAfterLoad(Throwable ex) {
        try {
            logFile.use--;
            if (cancelIndicator != null && cancelIndicator.get()) {
                finishWithCancel();
            } else if (groupConfig.getStopIndicator().get()) {
                finish(new StoppedException());
            } else if (ex != null) {
                finish(ex);
            } else {
                readBuffer.flip();
                parseContent();
            }
        } catch (Throwable e) {
            finish(e);
        }
    }

    private void discardBufferAndLoadNextFile(ByteBuffer buf) {
        buf.clear();
        long nextFileStartPos = logFiles.nextFilePos(bufferStartPos);
        loadLogFromStore(nextFileStartPos);
    }

    // return true if it should continue parse
    private boolean processHeader(ByteBuffer buf) {
        if (buf.remaining() >= LogHeader.ITEM_HEADER_SIZE) {
            if (!extractHeader(buf)) {
                // reached end of file
                discardBufferAndLoadNextFile(buf);
                return false;
            }
            crc32c.reset();
            state = STATE_BIZ_HEADER;
            if (result.size() > 0 && header.bodyLen + readBytes >= bytesLimit) {
                buf.position(buf.position() - LogHeader.ITEM_HEADER_SIZE);
                finish(result, bufferStartPos + buf.position());
                return false;
            } else {
                return true;
            }
        } else {
            long rest = logFile.endPos - bufferEndPos + buf.remaining();
            if (rest < LogHeader.ITEM_HEADER_SIZE) {
                // reached end of file
                discardBufferAndLoadNextFile(buf);
            } else {
                StoreUtil.prepareNextRead(buf);
                loadLogFromStore(bufferEndPos);
            }
            return false;
        }
    }

    private boolean extractHeader(ByteBuffer readBuffer) {
        LogHeader header = this.header;
        this.itemStartPos = bufferStartPos + readBuffer.position();
        header.read(readBuffer);
        if (header.isEndMagic()) {
            return false;
        }
        if (!header.crcMatch()) {
            throw new ChecksumException("header crc not match: index=" + (nextIndex + result.size())
                    + ",pos=" + itemStartPos + ",len=" + header.totalLen);
        }

        int bodyLen = header.bodyLen;
        if (!header.checkHeader(logFiles.filePos(itemStartPos), logFiles.fileLength())) {
            throw new RaftException("header check fail: index=" + (nextIndex + result.size()) + ",pos=" + itemStartPos);
        }

        LogItem li = new LogItem(heapPool);
        this.item = li;
        header.copy(li);


        int bizHeaderLen = header.bizHeaderLen;
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

    private boolean extractBizHeader(ByteBuffer buf) {
        int bizHeaderLen = header.bizHeaderLen;
        if (bizHeaderLen == 0) {
            state = STATE_BIZ_BODY;
            return true;
        }
        ByteBuffer destBuf = item.getHeaderBuffer();
        boolean readFinish = readData(buf, bizHeaderLen, destBuf);
        if (readFinish) {
            crc32c.reset();
            state = STATE_BIZ_BODY;
        } else {
            StoreUtil.prepareNextRead(buf);
            loadLogFromStore(bufferEndPos);
        }
        return readFinish;
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

    private boolean extractBizBody(ByteBuffer buf) {
        int bodyLen = header.bodyLen;
        if (bodyLen == 0) {
            result.add(item);
            item = null;
            state = STATE_ITEM_HEADER;
            return checkItemLimit(buf);
        }
        ByteBuffer destBuf = item.getBodyBuffer();
        boolean readFinish = readData(buf, bodyLen, destBuf);
        if (readFinish) {
            result.add(item);
            readBytes += bodyLen;
            item = null;
            state = STATE_ITEM_HEADER;
            return checkItemLimit(buf);
        } else {
            StoreUtil.prepareNextRead(buf);
            loadLogFromStore(bufferEndPos);
            return false;
        }
    }

    private boolean checkItemLimit(ByteBuffer buf) {
        if (result.size() >= limit) {
            finish(result, bufferStartPos + buf.position());
            return false;
        } else {
            return true;
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

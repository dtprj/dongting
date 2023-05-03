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
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftExecutor;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftLog;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class DefaultLogIterator implements RaftLog.LogIterator {
    private static final DtLog log = DtLogs.getLogger(DefaultLogIterator.class);

    private final IdxFileQueue idxFiles;
    private final LogFileQueue logFiles;
    private final RaftExecutor raftExecutor;
    private final ByteBufferPool directPool;
    final ByteBuffer readBuffer;

    final Supplier<Boolean> fullIndicator;
    final CRC32C crc32c = new CRC32C();
    final LogHeader header = new LogHeader();

    private long nextIndex = -1;
    private long nextPos = -1;

    private boolean error;
    private boolean close;

    private int bytes;
    private LogItem item;
    private int bodyLen;

    DefaultLogIterator(IdxFileQueue idxFiles, LogFileQueue logFiles, RaftExecutor raftExecutor,
                       ByteBufferPool directPool, Supplier<Boolean> fullIndicator) {
        this.idxFiles = idxFiles;
        this.logFiles = logFiles;
        this.raftExecutor = raftExecutor;
        this.directPool = directPool;
        this.readBuffer = directPool.borrow(1024 * 1024);
        this.readBuffer.limit(0);
        this.fullIndicator = fullIndicator;
    }

    @Override
    public CompletableFuture<List<LogItem>> next(long index, int limit, int bytesLimit) {
        try {
            if (error) {
                BugLog.getLog().error("iterator has error");
                throw new RaftException("iterator has error");
            }
            if (nextIndex == -1) {
                nextPos = idxFiles.syncLoadLogPos(index);
                nextIndex = index;
            } else {
                if (nextIndex != index) {
                    throw new RaftException("nextIndex!=index");
                }
            }
            return loadLog(limit, bytesLimit);
        } catch (Throwable e) {
            error = true;
            return CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<List<LogItem>> loadLog(int limit, int bytesLimit) {
        logFiles.checkPos(nextPos);
        List<LogItem> result = new ArrayList<>();
        resetBeforeLoad();
        if (readBuffer.hasRemaining()) {
            CompletableFuture<List<LogItem>> future = new CompletableFuture<>();
            extractItems(limit, bytesLimit, result, future);
            return future;
        } else {
            readBuffer.clear();
            CompletableFuture<List<LogItem>> future = new CompletableFuture<>();
            loadLogFromStore(nextPos, limit, bytesLimit, result, future);
            return future;
        }
    }

    private void extractItems(int limit, int bytesLimit, List<LogItem> result, CompletableFuture<List<LogItem>> future) {
        int oldRemaining = readBuffer.remaining();
        boolean extractFinish = extractItems(result, limit, bytesLimit);
        int extractBytes = oldRemaining - readBuffer.remaining();
        nextPos += extractBytes;
        if (extractFinish) {
            future.complete(result);
            nextIndex += result.size();
        } else {
            LogFileQueue.prepareNextRead(readBuffer);
            loadLogFromStore(nextPos, limit, bytesLimit, result, future);
        }
    }

    private boolean extractItems(List<LogItem> result, int limit, int bytesLimit) {
        ByteBuffer buf = readBuffer;
        while (buf.remaining() >= LogHeader.ITEM_HEADER_SIZE) {
            if (item == null) {
                if (extractedNewItem(result, limit, bytesLimit, buf)) {
                    return true;
                }
            } else {
                if (extractItemResume(result, limit, buf)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void loadLogFromStore(long pos, int limit, int bytesLimit,
                                  List<LogItem> result, CompletableFuture<List<LogItem>> future) {
        long rest = logFiles.getWritePos() - pos;
        if (rest <= 0) {
            error = true;
            log.error("rest is illegal. pos={}, writePos={}", pos, logFiles.getWritePos());
            future.completeExceptionally(new RaftException("rest is illegal."));
            return;
        }
        LogFile logFile = logFiles.getLogFile(pos);
        int fileStartPos = (int) (pos & LogFileQueue.FILE_LEN_MASK);
        rest = Math.min(rest, LogFileQueue.LOG_FILE_SIZE - fileStartPos);
        ByteBuffer readBuffer = this.readBuffer;
        if (rest < readBuffer.remaining()) {
            readBuffer.limit((int) (readBuffer.position() + rest));
        }
        AsyncIoTask t = new AsyncIoTask(readBuffer, fileStartPos, logFile, fullIndicator);
        logFile.use++;
        t.exec().whenCompleteAsync((v, ex) -> resumeAfterLoad(logFile, limit, bytesLimit,
                result, future, ex), raftExecutor);
    }

    private void resumeAfterLoad(LogFile logFile, int limit, int bytesLimit,
                                 List<LogItem> result, CompletableFuture<List<LogItem>> future, Throwable ex) {
        try {
            logFile.use--;
            if (fullIndicator.get()) {
                error = true;
                future.cancel(false);
            } else if (ex != null) {
                error = true;
                future.completeExceptionally(ex);
            } else {
                readBuffer.flip();
                extractItems(limit, bytesLimit, result, future);
            }
        } catch (Throwable e) {
            error = true;
            future.completeExceptionally(e);
        }
    }

    private boolean extractedNewItem(List<LogItem> result, int limit,
                                     int bytesLimit, ByteBuffer readBuffer) {
        LogHeader header = this.header;
        int startPos = readBuffer.position();
        header.read(readBuffer);

        int bodyLen = header.totalLen - header.bizHeaderLen;
        this.bodyLen = bodyLen;
        if (header.totalLen <= 0 || header.bizHeaderLen <= 0 ||  bodyLen < 0 || bodyLen > LogFileQueue.LOG_FILE_SIZE) {
            throw new RaftException("invalid log item length: " + header.totalLen
                    + "," + header.bizHeaderLen);
        }

        if (result.size() > 0 && bytes + bodyLen > bytesLimit) {
            // rollback position for next use
            readBuffer.position(startPos);
            return true;
        }

        LogItem li = new LogItem();
        this.item = li;
        li.setIndex(header.index);
        li.setType(header.type);
        li.setTerm(header.term);
        li.setPrevLogTerm(header.prevLogTerm);
        li.setTimestamp(header.timestamp);
        li.setDataSize(bodyLen);

        LogFileQueue.updateCrc(crc32c, readBuffer, startPos, );
        if (bodyLen > 0) {
            RefBuffer rbb = li.getBuffer();
            if (rbb == null) {
                rbb = heapPool.create(bodyLen);
                li.setBuffer(rbb);
            }
            int available = Math.min(readBuffer.remaining(), bodyLen);
            ByteBuffer dest = rbb.getBuffer();
            readBuffer.get(dest.array(), 0, available);
            dest.position(available);
        }
        if (readBuffer.remaining() >= bodyLen + 4) {
            if (bodyLen > 0) {
                li.getBuffer().getBuffer().flip();
            }
            result.add(li);
            it.bytes += bodyLen;
            it.resetItem();
            int crc = readBuffer.getInt(); // crc
            if (it.crc32c.getValue() != crc) {
                throw new RaftException("crc32c not match");
            }
            if (result.size() >= limit) {
                return true;
            }
        } else {
            // the rest bytes not greater than 4
        }
        return false;
    }

    private boolean extractItemResume(List<LogItem> result, int limit, ByteBuffer buf) {
        ByteBuffer destBuf = it.item.getBuffer().getBuffer();
        int read = destBuf.position();
        int restBytes = it.bodyLen - read;
        if (buf.remaining() >= restBytes) {
            updateCrc(it.crc32c, buf, buf.position(), restBytes);
            if (it.crc32c.getValue() != it.header.c) {
                throw new RaftException("crc32c not match");
            }
            buf.get(destBuf.array(), read, restBytes);
            destBuf.limit(it.bodyLen);
            destBuf.position(0);
            result.add(it.item);
            it.bytes += it.bodyLen;
            it.resetItem();
            if (result.size() >= limit) {
                return true;
            }
        } else {
            updateCrc(it.crc32c, buf, buf.position(), buf.limit());
            destBuf.put(buf);
        }
        return false;
    }

    @Override
    public void close() {
        if (close) {
            BugLog.getLog().error("iterator has closed");
        } else {
            directPool.release(readBuffer);
        }
        close = true;
    }

    public void resetBeforeLoad() {
        bytes = 0;
        resetItem();
    }

    public void resetItem() {
        item = null;
        bodyLen = 0;
    }
}

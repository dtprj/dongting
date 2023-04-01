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

import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.raft.client.RaftException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

/**
 * @author huangli
 */
public class IdxFileQueue extends FileQueue {
    private static final int ITEM_LEN = 8;
    private static final int IDX_FILE_SIZE = 8 * 1024 * 1024;
    private static final int MAX_CACHE_ITEMS = 16 * 1024;
    private static final int FLUSH_ITEMS = MAX_CACHE_ITEMS / 2;
    private static final int REMOVE_ITEMS = 512;
    private final LongLongSeqMap cache = new LongLongSeqMap();
    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(FLUSH_ITEMS * ITEM_LEN);

    private long persistIndex;
    private long lastIndex;
    private long firstIndex;

    public IdxFileQueue(Executor ioExecutor) {
        super(ioExecutor);
    }

    @Override
    protected long getFileSize() {
        return IDX_FILE_SIZE;
    }

    public void init(File dir, long persistIndex) throws IOException {
        super.init(dir);
        this.persistIndex = persistIndex;
        this.lastIndex = persistIndex;
        this.firstIndex = posToIndex(queueStartPosition);
    }

    private static long indexToPos(long index) {
        // each item 8 bytes
        return index << 3;
    }

    private static long posToIndex(long pos) {
        // each item 8 bytes
        return pos >>> 3;
    }

    public long findLogPosByItemIndex(long itemIndex) throws IOException {
        checkIndex(itemIndex);
        if (itemIndex < firstIndex) {
            return -1;
        }
        long result = cache.get(itemIndex);
        if (result > 0) {
            return result;
        }
        long pos = indexToPos(itemIndex);
        long filePos = pos % IDX_FILE_SIZE;
        LogFile lf = getLogFile(pos);
        // TODO reuse it
        ByteBuffer buffer = ByteBuffer.allocate(8);
        while (buffer.hasRemaining()) {
            lf.channel.read(buffer, filePos);
        }
        buffer.flip();
        return buffer.getLong();
    }

    private void checkIndex(long index) {
        if (index > lastIndex + 1) {
            BugLog.getLog().error("index is too large : lastIndex={}, index={}", lastIndex, index);
            throw new RaftException("index is too large");
        }
    }

    public void put(long itemIndex, long dataPosition) throws IOException {
        checkIndex(itemIndex);
        if (itemIndex < firstIndex) {
            BugLog.getLog().error("index is too small : firstIndex={}, index={}", firstIndex, itemIndex);
            throw new RaftException("index is too small");
        }
        if (cache.size() >= MAX_CACHE_ITEMS) {
            cache.remove(REMOVE_ITEMS);
        }
        cache.put(itemIndex, dataPosition);
        lastIndex = itemIndex;
        if (cache.getLastKey() - persistIndex >= FLUSH_ITEMS) {
            writeAndFlush();
        }
    }

    private void writeAndFlush() throws IOException {
        writeBuffer.clear();
        long index = persistIndex + 1;
        long startPos = indexToPos(index);
        for (int i = 0; i < FLUSH_ITEMS; i++, index++) {
            long value = cache.get(index);
            writeBuffer.putLong(value);
        }
        writeBuffer.flip();
        LogFile logFile = getLogFile(startPos);
        long posOfFile = startPos % IDX_FILE_SIZE;
        logFile.channel.write(writeBuffer, posOfFile);
        logFile.channel.force(false);
        cache.remove(FLUSH_ITEMS);
        persistIndex = index - 1;
    }
}

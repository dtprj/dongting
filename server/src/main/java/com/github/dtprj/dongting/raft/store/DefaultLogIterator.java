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
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftLog;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class DefaultLogIterator implements RaftLog.LogIterator {

    private final DefaultRaftLog defaultRaftLog;
    private final ByteBufferPool directPool;
    final ByteBuffer readBuffer;

    final Supplier<Boolean> fullIndicator;
    final CRC32C crc32c = new CRC32C();
    final LogHeader header = new LogHeader();

    long nextIndex = 1;

    int bytes;
    LogItem item;
    int bodyLen;

    DefaultLogIterator(DefaultRaftLog defaultRaftLog, ByteBufferPool directPool, Supplier<Boolean> fullIndicator) {
        this.defaultRaftLog = defaultRaftLog;
        this.directPool = directPool;
        this.readBuffer = directPool.borrow(1024 * 1024);
        this.fullIndicator = fullIndicator;
    }

    @Override
    public CompletableFuture<List<LogItem>> next(long index, int limit, int bytesLimit) {
        return defaultRaftLog.next(this, index, limit, bytesLimit);
    }

    @Override
    public void close() {
        directPool.release(readBuffer);
    }

    public void resetBuffer() {
        readBuffer.clear();
        readBuffer.position(0);
    }

    public void resetBeforeLoad() {
        bytes = 0;
        resetItem();
    }

    public void resetItem() {
        item = null;
        crc32c.reset();
        bodyLen = 0;
    }
}

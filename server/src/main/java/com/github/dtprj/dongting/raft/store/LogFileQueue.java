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

import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class LogFileQueue extends FileQueue {
    private static final DtLog log = DtLogs.getLogger(LogFileQueue.class);

    public static final long DEFAULT_LOG_FILE_SIZE = 1024 * 1024 * 1024;
    public static final int DEFAULT_WRITE_BUFFER_SIZE = 128 * 1024;

    protected final RefBufferFactory heapPool;
    protected final RefBufferFactory directPool;

    private final IdxOps idxOps;

    private final Timestamp ts;
    private final ByteBuffer writeBuffer;

    private final LogAppender logAppender;

    public LogFileQueue(File dir, RaftGroupConfig groupConfig, IdxOps idxOps, RaftLog.AppendCallback callback,
                        long fileSize, int writeBufferSize) {
        super(dir, groupConfig, fileSize);
        this.idxOps = idxOps;
        this.ts = groupConfig.getTs();

        this.heapPool = groupConfig.getHeapPool();
        this.directPool = groupConfig.getDirectPool();

        this.writeBuffer = ByteBuffer.allocateDirect(writeBufferSize);
        this.logAppender = new LogAppender(idxOps, this, groupConfig, writeBuffer, callback);
    }

    public long fileLength() {
        return fileSize;
    }

    public FiberFrame<Integer> restore(long restoreIndex, long restoreIndexPos, long firstValidPos,
                                       Supplier<Boolean> stopIndicator) {
        return null;
    }
}

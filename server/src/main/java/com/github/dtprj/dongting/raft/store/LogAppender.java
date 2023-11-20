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

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class LogAppender {
    private static final DtLog log = DtLogs.getLogger(LogAppender.class);
    private static final int STATE_WRITE_ITEM_HEADER = 0;
    private static final int STATE_WRITE_BIZ_HEADER = 1;
    private static final int STATE_WRITE_BIZ_BODY = 2;

    private static final int APPEND_OK = 100;
    private static final int CHANGE_BUFFER = 200;
    private static final int CHANGE_FILE = 300;

    private static final int MIN_WRITE_SIZE = 1024;

    private final IdxOps idxOps;
    private final LogFileQueue logFileQueue;
    private final RaftCodecFactory codecFactory;
    private final RaftGroupConfig groupConfig;
    private final ByteBuffer writeBuffer;
    private final CRC32C crc32c = new CRC32C();
    private final EncodeContext encodeContext;
    private final long fileLenMask;
    private final long bufferLenMask;
    private final RaftStatusImpl raftStatus;
    private final RaftLog.AppendCallback appendCallback;

    private long nextPersistIndex = -1;
    private long nextPersistPos = -1;

    private int state;
    private int pendingBytes;
    @SuppressWarnings("rawtypes")
    private Encoder currentHeaderEncoder;
    @SuppressWarnings("rawtypes")
    private Encoder currentBodyEncoder;

    private long bufferWriteAccumulatePos;

    private final IndexedQueue<WriteTask> writeTaskQueue = new IndexedQueue<>(32);

    LogAppender(IdxOps idxOps, LogFileQueue logFileQueue, RaftGroupConfig groupConfig,
                ByteBuffer writeBuffer, RaftLog.AppendCallback appendCallback) {
        this.idxOps = idxOps;
        this.logFileQueue = logFileQueue;
        this.codecFactory = groupConfig.getCodecFactory();
        this.encodeContext = new EncodeContext(groupConfig.getHeapPool());
        this.fileLenMask = logFileQueue.fileLength() - 1;
        this.groupConfig = groupConfig;
        this.writeBuffer = writeBuffer;
        this.bufferLenMask = writeBuffer.capacity() - 1;
        this.raftStatus = (RaftStatusImpl) groupConfig.getRaftStatus();
        this.appendCallback = appendCallback;
    }

    static class WriteTask {
        final LogFile logFile;
        final ByteBuffer buffer;
        final long writeStartPosInFile;
        final int writeStartPosInBuffer;
        final long startIndex;
        LogItem lastItem;
        boolean finished;
        int size;
        int retryCount;

        public WriteTask(LogFile logFile, ByteBuffer buffer, long startIndex,
                         long writeStartPosInFile, int writeStartPosInBuffer) {
            this.logFile = logFile;
            this.buffer = buffer;
            this.startIndex = startIndex;
            this.writeStartPosInFile = writeStartPosInFile;
            this.writeStartPosInBuffer = writeStartPosInBuffer;
        }
    }

}

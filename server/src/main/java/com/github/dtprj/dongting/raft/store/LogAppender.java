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

import com.github.dtprj.dongting.buf.Buffers;
import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.PerfConsts;
import com.github.dtprj.dongting.fiber.DispatcherThread;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftReqData;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class LogAppender {
    private static final DtLog log = DtLogs.getLogger(LogAppender.class);

    private final IdxOps idxOps;
    private final LogFileQueue logFileQueue;
    private final CRC32C crc32c = new CRC32C();
    private final EncodeContext encodeContext;
    private final long fileLenMask;
    private final RaftStatusImpl raftStatus;

    private final Buffers buffers;

    // update before write operation issued
    long nextPersistIndex = -1;
    long nextPersistPos = -1;

    private final PerfCallback perfCallback;
    final ChainWriter chainWriter;

    LogAppender(IdxOps idxOps, LogFileQueue logFileQueue, RaftGroupConfigEx groupConfig, ChainWriter chainWriter) {
        this.idxOps = idxOps;
        this.logFileQueue = logFileQueue;
        this.raftStatus = (RaftStatusImpl) groupConfig.raftStatus;
        this.chainWriter = chainWriter;

        DispatcherThread thread = groupConfig.fiberGroup.dispatcher.thread;
        this.buffers = thread.buffers;
        this.encodeContext = new EncodeContext(thread.buffers);
        this.fileLenMask = logFileQueue.fileLength() - 1;
        this.perfCallback = groupConfig.perfCallback;
    }

    public void startFiber() {
        chainWriter.start();
    }

    public FiberFuture<Void> close() {
        return chainWriter.stop();
    }

    class WriteFiberFrame extends FiberFrame<Void> {

        // lastItem/writeCount/bytesToWrite are reset in encodeAndWriteItems();
        private RaftTask lastItem;
        private int writeCount;
        private int bytesToWrite;

        // bufRef/buffer are reassigned on every entry to encodeAndWriteItems()
        private RefBuffer bufRef;
        private ByteBuffer buffer;

        private final List<RaftTask> taskList;

        WriteFiberFrame(List<RaftTask> taskList) {
            this.taskList = taskList;
        }

        @Override
        protected FrameCallResult handle(Throwable ex) {
            if (raftStatus.installSnapshot) {
                log.error("log writer error, ignore it since install snapshot is true", ex);
                return Fiber.frameReturn();
            } else {
                throw Fiber.fatal(ex);
            }
        }

        private boolean shouldReturn() {
            return logFileQueue.isMarkClose() || raftStatus.installSnapshot;
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (taskList.isEmpty() || shouldReturn()) {
                return Fiber.frameReturn();
            }
            if (idxOps.needWaitFlush()) {
                long start = perfCallback.takeTime(PerfConsts.RAFT_D_IDX_BLOCK);
                return Fiber.call(idxOps.waitFlush(), v -> afterIdxReady(start));
            }
            return ensureWritePosReady(0);
        }

        private FrameCallResult afterIdxReady(long perfStartTime) {
            perfCallback.fireTime(PerfConsts.RAFT_D_IDX_BLOCK, perfStartTime);
            return Fiber.resume(null, this);
        }

        private FrameCallResult ensureWritePosReady(int taskIndex) {
            if (shouldReturn()) {
                return Fiber.frameReturn();
            }
            return Fiber.call(logFileQueue.ensureWritePosReady(nextPersistPos), v -> afterWritePosReady(taskIndex));
        }

        private FrameCallResult afterWritePosReady(int taskIndex) {
            if (shouldReturn()) {
                return Fiber.frameReturn();
            }
            LogFile lf = logFileQueue.getLogFile(nextPersistPos);
            if (lf.isDeleted()) {
                BugLog.log("file is deleted or mark deleted: {}", lf.getFile().getPath());
                throw new RaftException("file is deleted or mark deleted: " + lf.getFile().getPath());
            }
            return encodeAndWriteItems(lf, taskIndex);
        }

        private FrameCallResult encodeAndWriteItems(LogFile file, int taskIndex) {
            long roundStartTime = perfCallback.takeTimeAndRefresh(PerfConsts.RAFT_D_ENCODE_AND_WRITE, raftStatus.ts);
            bytesToWrite = 0;
            lastItem = null;
            writeCount = 0;

            boolean writeEndHeader = false;
            boolean rollNextFile = false;
            long fileRestBytes = file.endPos - nextPersistPos;
            int count = 0;
            for (int listSize = taskList.size(), i = taskIndex; i < listSize; i++) {
                RaftTask li = taskList.get(i);
                int len = li.reqData.totalLen;
                if (len <= fileRestBytes) {
                    bytesToWrite += len;
                    fileRestBytes -= len;
                    count++;
                } else {
                    rollNextFile = true;
                    // file rest bytes not enough
                    if (fileRestBytes >= LogHeader.ITEM_HEADER_SIZE) {
                        writeEndHeader = true;
                        bytesToWrite += LogHeader.ITEM_HEADER_SIZE;
                    }
                    break;
                }
            }

            bufRef = borrowBuffer(bytesToWrite);
            buffer = bufRef.getBuffer();
            try {
                encodeItems(taskIndex, count, file);

                if (writeEndHeader) {
                    if (buffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                        doWrite(file);
                    }
                    LogHeader.writeEndHeader(crc32c, buffer);
                }
                if (buffer.position() > 0) {
                    doWrite(file);
                } else {
                    if (buffer.capacity() > 0) {
                        BugLog.log("buffer capacity > 0", buffer.capacity());
                    }
                }
            } finally {
                if (bufRef != null) {
                    bufRef.release();
                    bufRef = null;
                    buffer = null;
                }
            }

            if (nextPersistPos == file.endPos) {
                log.info("current file {} has no enough space, nextPersistPos is {}, next file start pos is {}",
                        file.getFile().getName(), nextPersistPos, nextPersistPos);
            } else if (rollNextFile) {
                // prepare to write new file
                long next = logFileQueue.nextFilePos(nextPersistPos);
                log.info("current file {} has no enough space, nextPersistPos is {}, next file start pos is {}",
                        file.getFile().getName(), nextPersistPos, next);
                nextPersistPos = next;
            }
            perfCallback.fireTimeAndRefresh(PerfConsts.RAFT_D_ENCODE_AND_WRITE, roundStartTime, 1, 0, raftStatus.ts);

            if (taskIndex + count == taskList.size()) {
                return Fiber.frameReturn();
            } else {
                // continue loop
                int newTaskIndex = taskIndex + count;
                return Fiber.resume(null, v -> ensureWritePosReady(newTaskIndex));
            }
        }

        private void encodeItems(int startTaskIndex, int count, LogFile file) {
            long writeStartPosInFile = nextPersistPos & fileLenMask;
            long dataPos = file.startPos + writeStartPosInFile;
            for (int i = 0; i < count; i++) {
                RaftTask li = taskList.get(startTaskIndex + i);
                RaftReqData rd = li.reqData;
                if (file.firstIndex == 0) {
                    file.firstIndex = rd.index;
                    file.firstTerm = rd.term;
                    file.firstTimestamp = rd.timestamp;
                }

                int len = li.actualSize();
                encodeData(len, li, file);
                idxOps.put(rd.index, dataPos, rd.timestamp, len);
                dataPos += len;
                lastItem = li;
                writeCount++;
            }
        }

        private void encodeData(int actualSize, RaftTask src, LogFile file) {
            try {
                int totalEncodeLen = 0;
                while (true) {
                    int startPos = buffer.position();
                    boolean finish = src.encode(encodeContext, buffer);
                    totalEncodeLen += buffer.position() - startPos;
                    if (finish) {
                        if (totalEncodeLen != actualSize) {
                            throw new RaftException("encode problem, totalEncodeLen != actualSize");
                        }
                        break;
                    } else {
                        doWrite(file);
                    }
                }
            } finally {
                encodeContext.reset();
            }
        }

        private void doWrite(LogFile file) {
            buffer.flip();
            int bytes = buffer.remaining();

            long lastIndex = lastItem != null ? lastItem.reqData.index : -1;
            long writeStartPosInFile = nextPersistPos & fileLenMask;

            RefBuffer refBufferCopy = this.bufRef;
            this.bufRef = null;
            this.buffer = null;

            // ownership transferred
            chainWriter.submitWrite(file, refBufferCopy, writeStartPosInFile,
                    lastItem != null, writeCount, lastIndex);

            nextPersistPos += bytes;
            nextPersistIndex += writeCount;

            bytesToWrite -= bytes;
            lastItem = null;
            writeCount = 0;

            bufRef = borrowBuffer(bytesToWrite);
            buffer = bufRef.getBuffer();
        }

        private RefBuffer borrowBuffer(int size) {
            if (size == 0) {
                return RefBuffer.EMPTY;
            }
            size = Math.min(size, logFileQueue.maxWriteBufferSize);
            return buffers.borrowDirectLocal(size);
        }
    }

    public void setNext(long nextPersistIndex, long nextPersistPos) {
        this.nextPersistIndex = nextPersistIndex;
        this.nextPersistPos = nextPersistPos;
    }

}

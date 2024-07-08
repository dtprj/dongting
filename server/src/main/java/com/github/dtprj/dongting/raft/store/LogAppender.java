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
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.common.DtThread;
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FiberInterruptException;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.PerfConsts;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftTask;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
class LogAppender {
    private static final DtLog log = DtLogs.getLogger(LogAppender.class);
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);

    private final IdxOps idxOps;
    private final LogFileQueue logFileQueue;
    private final RaftGroupConfigEx groupConfig;
    private final CRC32C crc32c = new CRC32C();
    private final EncodeContext encodeContext;
    private final long fileLenMask;

    private final ByteBufferPool directPool;

    // update before write operation issued
    long nextPersistIndex = -1;
    long nextPersistPos = -1;

    private final Fiber appendFiber;

    private final Supplier<Boolean> writeStopIndicator;

    private final PerfCallback perfCallback;
    final ChainWriter chainWriter;

    private final FiberChannel<RaftTask> taskChannel;

    LogAppender(IdxOps idxOps, LogFileQueue logFileQueue, RaftGroupConfigEx groupConfig, ChainWriter chainWriter) {
        this.idxOps = idxOps;
        this.logFileQueue = logFileQueue;
        this.chainWriter = chainWriter;
        this.groupConfig = groupConfig;

        DtThread thread = groupConfig.getFiberGroup().getThread();
        this.directPool = thread.getDirectPool();
        this.encodeContext = new EncodeContext(thread.getHeapPool());
        this.fileLenMask = logFileQueue.fileLength() - 1;
        FiberGroup fiberGroup = groupConfig.getFiberGroup();
        WriteFiberFrame writeFiberFrame = new WriteFiberFrame();
        this.appendFiber = new Fiber("append-" + groupConfig.getGroupId(), fiberGroup, writeFiberFrame);
        this.writeStopIndicator = logFileQueue::isClosed;
        this.perfCallback = groupConfig.getPerfCallback();
        this.taskChannel = fiberGroup.newChannel();
    }

    public void startFiber() {
        appendFiber.start();
        chainWriter.startForceFiber();
    }

    public FiberFuture<Void> close() {
        appendFiber.interrupt();
        FiberFuture<Void> closeFuture = groupConfig.getFiberGroup().newFuture("appenderClose");
        FiberFuture<Void> f1;
        if (appendFiber.isStarted()) {
            f1 = appendFiber.join();
        } else {
            f1 = FiberFuture.completedFuture(groupConfig.getFiberGroup(), null);
        }
        f1.registerCallback((v, ex) -> {
            if (ex != null) {
                closeFuture.completeExceptionally(ex);
            } else {
                chainWriter.shutdownForceFiber().registerCallback((v2, ex2) -> {
                    if (ex2 != null) {
                        closeFuture.completeExceptionally(ex2);
                    } else {
                        closeFuture.complete(null);
                    }
                });
            }
        });
        return closeFuture;
    }

    public void submit(List<RaftTask> taskList) {
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, len = taskList.size(); i < len; i++) {
            RaftTask task = taskList.get(i);
            taskChannel.offer(task);
        }
    }

    private class WriteFiberFrame extends FiberFrame<Void> {

        // 3 temp status fields, should reset in encodeAndWriteItems()
        private LogItem lastItem;
        private int writeCount;
        private int bytesToWrite;

        private final ArrayList<RaftTask> taskList = new ArrayList<>(64);

        @Override
        protected FrameCallResult handle(Throwable ex) {
            if (ex instanceof FiberInterruptException) {
                return Fiber.frameReturn();
            }
            throw Fiber.fatal(ex);
        }

        @Override
        public FrameCallResult execute(Void input) {
            if (logFileQueue.isClosed()) {
                return Fiber.frameReturn();
            }
            if (idxOps.needWaitFlush()) {
                long start = perfCallback.takeTime(PerfConsts.RAFT_D_IDX_BLOCK);
                return Fiber.call(idxOps.waitFlush(), v -> afterIdxReady(start));
            }
            return taskChannel.takeAll(taskList, this::afterTakeAll);
        }

        private FrameCallResult afterIdxReady(long perfStartTime) {
            perfCallback.fireTime(PerfConsts.RAFT_D_IDX_BLOCK, perfStartTime);
            return Fiber.resume(null, this);
        }

        private FrameCallResult afterTakeAll(Void unused) {
            if (taskList.isEmpty()) {
                return Fiber.resume(null, this);
            }
            return ensureWritePosReady(0);
        }

        private FrameCallResult ensureWritePosReady(int taskIndex) {
            if (logFileQueue.isClosed()) {
                return Fiber.frameReturn();
            }
            return Fiber.call(logFileQueue.ensureWritePosReady(nextPersistPos), v -> afterWritePosReady(taskIndex));
        }

        private FrameCallResult afterWritePosReady(int taskIndex) {
            if (logFileQueue.isClosed()) {
                return Fiber.frameReturn();
            }
            LogFile lf = logFileQueue.getLogFile(nextPersistPos);
            if (lf.isDeleted()) {
                BugLog.getLog().error("file is deleted or mark deleted: {}", lf.getFile().getPath());
                throw new RaftException("file is deleted or mark deleted: " + lf.getFile().getPath());
            }
            return encodeAndWriteItems(lf, taskIndex);
        }

        private FrameCallResult encodeAndWriteItems(LogFile file, int taskIndex) {
            long roundStartTime = perfCallback.takeTime(PerfConsts.RAFT_D_ENCODE_AND_WRITE);
            bytesToWrite = 0;
            lastItem = null;
            writeCount = 0;

            boolean writeEndHeader = false;
            boolean rollNextFile = false;
            long fileRestBytes = file.endPos - nextPersistPos;
            int count = 0;
            for (int listSize = taskList.size(), i = taskIndex; i < listSize; i++) {
                LogItem li = taskList.get(i).getItem();
                int len = LogHeader.computeTotalLen(0, li.getActualHeaderSize(), li.getActualBodySize());
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

            ByteBuffer buffer = borrowBuffer(bytesToWrite);
            buffer = encodeItems(taskIndex, count, file, buffer);

            if (writeEndHeader) {
                if (buffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                    buffer = doWrite(file, buffer);
                }
                LogHeader.writeEndHeader(crc32c, buffer);
            }
            if (buffer.position() > 0) {
                doWrite(file, buffer);
            } else {
                if (buffer.capacity() > 0) {
                    BugLog.getLog().error("buffer capacity > 0", buffer.capacity());
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
            perfCallback.fireTime(PerfConsts.RAFT_D_ENCODE_AND_WRITE, roundStartTime);

            // continue loop
            if (taskIndex + count == taskList.size()) {
                taskList.clear();
                return Fiber.resume(null, this);
            } else {
                int newTaskIndex = taskIndex + count;
                return Fiber.resume(null, v -> ensureWritePosReady(newTaskIndex));
            }
        }

        private ByteBuffer encodeItems(int startTaskIndex, int count, LogFile file, ByteBuffer buffer) {
            long writeStartPosInFile = nextPersistPos & fileLenMask;
            long dataPos = file.startPos + writeStartPosInFile;
            for (int i = 0; i < count; i++) {
                LogItem li = taskList.get(startTaskIndex + i).getItem();
                if (file.firstIndex == 0) {
                    file.firstIndex = li.getIndex();
                    file.firstTerm = li.getTerm();
                    file.firstTimestamp = li.getTimestamp();
                }
                if (buffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                    buffer = doWrite(file, buffer);
                }
                int len = LogHeader.writeHeader(crc32c, buffer, li);

                if (li.getActualHeaderSize() > 0) {
                    if (!buffer.hasRemaining()) {
                        buffer = doWrite(file, buffer);
                    }
                    buffer = encodeData(li.getActualHeaderSize(), li.getHeader(), buffer, file);
                }
                if (li.getActualBodySize() > 0) {
                    if (!buffer.hasRemaining()) {
                        buffer = doWrite(file, buffer);
                    }
                    buffer = encodeData(li.getActualBodySize(), li.getBody(), buffer, file);
                }

                idxOps.put(li.getIndex(), dataPos);
                dataPos += len;
                lastItem = li;
                writeCount++;
            }
            return buffer;
        }

        private ByteBuffer encodeData(int actualSize, Encodable src, ByteBuffer dest, LogFile file) {
            crc32c.reset();
            try {
                int totalEncodeLen = 0;
                while (true) {
                    int startPos = dest.position();
                    boolean finish = src.encode(encodeContext, dest);
                    totalEncodeLen += dest.position() - startPos;
                    RaftUtil.updateCrc(crc32c, dest, startPos, dest.position() - startPos);
                    if (finish) {
                        if (totalEncodeLen != actualSize) {
                            throw new RaftException("encode problem, totalEncodeLen != actualSize");
                        }
                        break;
                    } else {
                        dest = doWrite(file, dest);
                    }
                }
            } finally {
                encodeContext.reset();
            }
            if (dest.remaining() < 4) {
                dest = doWrite(file, dest);
            }
            dest.putInt((int) crc32c.getValue());
            return dest;
        }

        private ByteBuffer doWrite(LogFile file, ByteBuffer buffer) {
            buffer.flip();
            int bytes = buffer.remaining();

            long lastIndex = lastItem != null ? lastItem.getIndex() : -1;
            long writeStartPosInFile = nextPersistPos & fileLenMask;
            int[] retryInterval = logFileQueue.initialized ? groupConfig.getIoRetryInterval() : null;
            ChainWriter.WriteTask task = new ChainWriter.WriteTask(groupConfig.getFiberGroup(), file, retryInterval, true,
                    writeStopIndicator, buffer, writeStartPosInFile, lastItem != null, writeCount, lastIndex);
            chainWriter.submitWrite(task);

            nextPersistPos += bytes;
            nextPersistIndex += writeCount;

            bytesToWrite -= bytes;
            lastItem = null;
            writeCount = 0;

            return borrowBuffer(bytesToWrite);
        }

        private ByteBuffer borrowBuffer(int size) {
            if (size == 0) {
                return EMPTY_BUFFER;
            } else {
                size = Math.min(size, logFileQueue.maxWriteBufferSize);
                return directPool.borrow(size);
            }
        }
    }

    public void setNext(long nextPersistIndex, long nextPersistPos) {
        this.nextPersistIndex = nextPersistIndex;
        this.nextPersistPos = nextPersistPos;
    }

}

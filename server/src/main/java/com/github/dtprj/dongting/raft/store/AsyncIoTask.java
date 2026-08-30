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

import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AsyncIoTask {
    private static final DtLog log = DtLogs.getLogger(AsyncIoTask.class);
    private final DtFile dtFile;
    private final Supplier<Boolean> cancelRetryIndicator;
    private final FiberFuture<Void> future;

    private final int[] retryInterval;
    private final FiberGroup fiberGroup;

    private ByteBuffer ioBuffer;
    // non-null only for gathering write
    private ByteBuffer[] ioBuffers;
    private long filePos;

    private boolean write;
    private boolean force;

    private int retryCount = 0;

    private boolean rwCalled;

    public AsyncIoTask(FiberGroup fiberGroup, DtFile dtFile) {
        this(fiberGroup, dtFile, null, null);
    }

    public AsyncIoTask(FiberGroup fiberGroup, DtFile dtFile, int[] retryInterval,
                       Supplier<Boolean> cancelRetryIndicator) {
        this.fiberGroup = fiberGroup;
        Objects.requireNonNull(dtFile);
        this.dtFile = dtFile;
        this.retryInterval = retryInterval;
        this.cancelRetryIndicator = cancelRetryIndicator;
        this.future = fiberGroup.newFuture("asyncIoTaskFuture");
    }

    public FiberFuture<Void> read(ByteBuffer ioBuffer, long filePos) {
        checkPosition(ioBuffer);
        this.ioBuffer = ioBuffer;
        return exec(filePos, false);
    }

    public FiberFuture<Void> write(ByteBuffer ioBuffer, long filePos) {
        checkPosition(ioBuffer);
        this.ioBuffer = ioBuffer;
        return exec(filePos, true);
    }

    public FiberFuture<Void> write(ByteBuffer[] ioBuffers, long filePos) {
        Objects.requireNonNull(ioBuffers);
        for (ByteBuffer buf : ioBuffers) {
            checkPosition(buf);
        }
        this.ioBuffers = ioBuffers;
        return exec(filePos, true);
    }

    /**
     * Writes and then forces (fdatasync) in the same io turn: the future completes
     * only when the data is durable.
     */
    public FiberFuture<Void> writeAndForce(ByteBuffer ioBuffer, long filePos) {
        this.force = true;
        return write(ioBuffer, filePos);
    }

    /**
     * Submits a force (fdatasync), recognized by both buffers being null. A closed
     * channel is re-opened first: skipping the force would fake durability.
     */
    public FiberFuture<Void> force() {
        return exec(0, false);
    }

    private static void checkPosition(ByteBuffer buf) {
        if (buf.position() != 0) {
            throw new RaftException("buffer position must be 0: " + buf.position());
        }
    }

    private FiberFuture<Void> exec(long filePos, boolean write) {
        if (rwCalled) {
            future.completeExceptionally(new RaftException("io task can't reused"));
            return future;
        }
        Fiber f = Fiber.currentFiber();
        if (f == null || f.isDaemon()) {
            throw new RaftException("io task should not run in daemon fiber");
        }
        this.filePos = filePos;
        this.write = write;
        ensureOpenThenExec();
        rwCalled = true;
        return future;
    }

    private void ensureOpenThenExec() {
        if (!dtFile.isRwChannelOpen()) {
            FiberFuture<Void> openFut = dtFile.ensureOpen();
            openFut.registerCallback((v, ex) -> {
                if (ex != null) {
                    retry(ex);
                } else {
                    exec(filePos);
                }
            });
        } else {
            exec(filePos);
        }
    }

    protected void fireComplete(Throwable ex) {
        if (ex == null) {
            future.fireComplete(null);
        } else {
            String op;
            if (ioBuffer == null && ioBuffers == null) {
                op = "force";
            } else if (write) {
                op = force ? "writeAndForce" : "write";
            } else {
                op = "read";
            }
            String s = op + " file=" + dtFile.getFile().getPath() + ", filePos=" + filePos + " fail. " + ex.getMessage();
            future.fireCompleteExceptionally(new IOException(s, ex));
        }
    }

    void retry(Throwable ioEx) {
        long sleepTime = StoreUtil.calcRetryInterval(retryCount, retryInterval);
        if (sleepTime <= 0) {
            // no retryInterval or retry budget exhausted
            fireComplete(ioEx);
            return;
        }

        Fiber retryFiber = new Fiber("io-retry-fiber", fiberGroup, new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                log.warn("io error, retry after {} ms", sleepTime, ioEx);
                return Fiber.sleepUntilShouldStop(sleepTime, this::resume);
            }

            private FrameCallResult resume(Void v) {
                if (shouldCancelRetry()) {
                    fireComplete(ioEx);
                    return Fiber.frameReturn();
                }
                retryCount++;
                if (ioBuffers != null) {
                    for (ByteBuffer buf : ioBuffers) {
                        buf.rewind();
                    }
                } else if (ioBuffer != null) {
                    ioBuffer.rewind();
                }
                // the channel may have been idle-closed meanwhile
                ensureOpenThenExec();
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                log.error("unexpected retry error", ex);
                fireComplete(ex);
                return Fiber.frameReturn();
            }

            private boolean shouldCancelRetry() {
                if (isGroupShouldStopPlain()) {
                    // if fiber group is stopped, ignore cancelIndicator
                    return true;
                }
                if (dtFile.destroyed) {
                    // a destroyed file can never be opened again, retrying is futile
                    log.warn("retry canceled because file is destroyed");
                    return true;
                }
                if (cancelRetryIndicator != null && cancelRetryIndicator.get()) {
                    log.warn("retry canceled by cancelIndicator");
                    return true;
                }
                return false;
            }
        });
        if (!fiberGroup.fireFiber(retryFiber)) {
            future.fireCompleteExceptionally(new RaftException("retry failed because fiber group is stopped"));
        }
    }

    private void exec(long pos) {
        try {
            dtFile.ioExecutor.execute(() -> doExec(pos));
        } catch (Throwable e) {
            fireComplete(e);
        }
    }

    // this method set to protected for mock error in unit test
    protected void doExec(long pos) {
        try {
            if (ioBuffer == null && ioBuffers == null) {
                dtFile.doForce(false);
                fireComplete(null);
                return;
            }
            if (ioBuffers != null) {
                doGatheringWrite(pos);
            } else {
                while (ioBuffer.hasRemaining()) {
                    int n;
                    if (write) {
                        n = dtFile.getChannel().write(ioBuffer, pos);
                    } else {
                        n = dtFile.getChannel().read(ioBuffer, pos);
                    }
                    if (n < 0) {
                        if (write) {
                            throw new IOException("write returned " + n);
                        }
                        fireComplete(new RaftException("read end of file"));
                        return;
                    }
                    if (n == 0) {
                        // both read and write may return 0, just retry the syscall
                        log.warn("{} returned 0 for file={}, pos={}",
                                write ? "write" : "read", dtFile.getFile().getPath(), pos);
                    }
                    pos = filePos + ioBuffer.position();
                }
            }
            if (force) {
                dtFile.doForce(false);
            }
            fireComplete(null);
        } catch (Throwable e) {
            retry(e);
        }
    }

    private void doGatheringWrite(long pos) throws IOException {
        FileChannel channel = dtFile.getChannel();
        // gathering write is only used for raft log files
        ReentrantLock lock = ((LogFile) dtFile).gatheringWriteLock;
        int offset = 0;
        int length = ioBuffers.length;
        while (length > 0) {
            if (!ioBuffers[offset].hasRemaining()) {
                offset++;
                length--;
                continue;
            }
            // gathering write of a single buffer is no better than positional write,
            // so only try the lock when at least 2 buffers remain
            if (length >= 2 && lock.tryLock()) {
                try {
                    // non-positional gathering write mutates the channel position,
                    // so it must be serialized with other gathering writes of the same file
                    channel.position(pos);
                    while (length > 0) {
                        if (ioBuffers[offset].hasRemaining()) {
                            long n = channel.write(ioBuffers, offset, length);
                            if (n < 0) {
                                throw new IOException("write returned " + n);
                            }
                            if (n == 0) {
                                // write may return 0, just retry the syscall
                                log.warn("gathering write returned 0 for file={}, pos={}",
                                        dtFile.getFile().getPath(), pos);
                            }
                        } else {
                            offset++;
                            length--;
                        }
                    }
                } finally {
                    lock.unlock();
                }
            } else {
                // another gathering write of the same file is in progress,
                // degrade this buffer to positional writes, and try the lock
                // again for remaining buffers since tryLock is very cheap
                ByteBuffer buf = ioBuffers[offset];
                while (buf.hasRemaining()) {
                    int n = channel.write(buf, pos);
                    if (n < 0) {
                        throw new IOException("write returned " + n);
                    }
                    if (n == 0) {
                        // write may return 0, just retry the syscall
                        log.warn("degraded positional write returned 0 for file={}, pos={}",
                                dtFile.getFile().getPath(), pos);
                    }
                    pos += n;
                }
                offset++;
                length--;
            }
        }
    }

    public FiberFuture<Void> getFuture() {
        return future;
    }

    public DtFile getDtFile() {
        return dtFile;
    }
}

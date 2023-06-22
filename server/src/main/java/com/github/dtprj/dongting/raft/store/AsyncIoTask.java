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

import com.github.dtprj.dongting.raft.server.RaftException;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AsyncIoTask implements CompletionHandler<Integer, CompletableFuture<Void>> {
    private final AsynchronousFileChannel channel;
    private final Supplier<Boolean> stopIndicator;

    private ByteBuffer ioBuffer;
    private long filePos;
    private boolean write;
    private int position;
    private boolean flush;
    private boolean flushMeta;

    public AsyncIoTask(AsynchronousFileChannel channel, Supplier<Boolean> stopIndicator) {
        this.channel = channel;
        this.stopIndicator = stopIndicator;
    }

    public CompletableFuture<Void> read(ByteBuffer ioBuffer, long filePos) {
        return exec(false, false, false, ioBuffer, filePos);
    }

    public CompletableFuture<Void> write(boolean flush, boolean flushMeta, ByteBuffer ioBuffer, long filePos) {
        return exec(true, flush, flushMeta, ioBuffer, filePos);
    }

    private CompletableFuture<Void> exec(boolean write, boolean flush, boolean flushMeta,
                                         ByteBuffer ioBuffer, long filePos) {
        this.write = write;
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.flush = flush;
        this.flushMeta = flushMeta;
        this.position = ioBuffer.position();

        CompletableFuture<Void> f = new CompletableFuture<>();
        exec(f, filePos);
        return f;
    }

    public CompletableFuture<Void> retry() {
        ioBuffer.position(position);
        CompletableFuture<Void> f = new CompletableFuture<>();
        exec(f, filePos);
        return f;
    }

    private void exec(CompletableFuture<Void> f, long pos) {
        try {
            if (write) {
                channel.write(ioBuffer, pos, f, this);
            } else {
                channel.read(ioBuffer, pos, f, this);
            }
        } catch (Throwable e) {
            f.completeExceptionally(e);
        }
    }

    @Override
    public void completed(Integer result, CompletableFuture<Void> f) {
        if (f.isCancelled()) {
            return;
        }
        if (result < 0) {
            f.completeExceptionally(new RaftException("read end of file"));
            return;
        }
        if (ioBuffer.hasRemaining()) {
            if (stopIndicator != null && stopIndicator.get()) {
                f.cancel(false);
                return;
            }
            int readBytes = ioBuffer.position() - position;
            exec(f, filePos + readBytes);
        } else {
            try {
                if (flush) {
                    channel.force(flushMeta);
                }
                f.complete(null);
            } catch (Throwable e) {
                f.completeExceptionally(e);
            }
        }
    }

    @Override
    public void failed(Throwable exc, CompletableFuture<Void> f) {
        f.completeExceptionally(exc);
    }
}

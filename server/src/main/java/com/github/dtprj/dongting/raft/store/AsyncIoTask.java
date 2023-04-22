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

import com.github.dtprj.dongting.raft.client.RaftException;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class AsyncIoTask implements CompletionHandler<Integer, CompletableFuture<Void>> {
    private final ByteBuffer ioBuffer;
    private final long startPos;
    final LogFile logFile;
    private final Supplier<Boolean> stopIndicator;
    private final boolean read;
    private final int position;

    public AsyncIoTask(boolean read, ByteBuffer ioBuffer, long startPos, LogFile logFile,
                       Supplier<Boolean> stopIndicator) {
        this.ioBuffer = ioBuffer;
        this.startPos = startPos;
        this.logFile = logFile;
        this.stopIndicator = stopIndicator;
        this.read = read;
        this.position = ioBuffer.position();
    }

    public CompletableFuture<Void> exec() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        exec(f, startPos);
        return f;
    }

    public CompletableFuture<Void> retry() {
        ioBuffer.position(position);
        return exec();
    }

    private void exec(CompletableFuture<Void> f, long pos) {
        try {
            if (read) {
                logFile.channel.read(ioBuffer, pos, f, this);
            } else {
                logFile.channel.write(ioBuffer, pos, f, this);
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
            exec(f, startPos + readBytes);
        } else {
            try {
                if (!read) {
                    logFile.channel.force(false);
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

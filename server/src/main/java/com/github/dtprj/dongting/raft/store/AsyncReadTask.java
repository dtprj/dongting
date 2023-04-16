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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class AsyncReadTask implements CompletionHandler<Integer, Void> {
    private final ByteBuffer readBuffer;
    private long pos;
    private final AsynchronousFileChannel channel;
    private final Supplier<Boolean> cancel;
    private final CompletableFuture<Void> f = new CompletableFuture<>();

    public AsyncReadTask(ByteBuffer readBuffer, long pos, AsynchronousFileChannel channel) {
        this(readBuffer, pos, channel, null);
    }

    public AsyncReadTask(ByteBuffer readBuffer, long pos, AsynchronousFileChannel channel, Supplier<Boolean> cancel) {
        this.readBuffer = readBuffer;
        this.pos = pos;
        this.channel = channel;
        this.cancel = cancel;
    }

    public CompletableFuture<Void> exec() {
        channel.read(readBuffer, pos, null, this);
        return f;
    }

    @Override
    public void completed(Integer result, Void attachment) {
        if (result == -1) {
            f.completeExceptionally(new IOException("read end of file"));
            return;
        }
        if (readBuffer.hasRemaining()) {
            pos += result;
            if (cancel != null && cancel.get()) {
                f.cancel(false);
                return;
            }
            exec();
        } else {
            f.complete(null);
        }
    }

    @Override
    public void failed(Throwable exc, Void attachment) {
        f.completeExceptionally(exc);
    }
}

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

import com.github.dtprj.dongting.fiber.FiberCancelException;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.raft.RaftException;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AsyncIoTask implements CompletionHandler<Integer, CompletionHandler<Void,Void>> {
    private final AsynchronousFileChannel channel;
    private final Supplier<Boolean> cancelIndicator;

    private ByteBuffer ioBuffer;
    private long filePos;
    private boolean write;
    private int position;
    private boolean flush;
    private boolean flushMeta;

    public AsyncIoTask(AsynchronousFileChannel channel, Supplier<Boolean> cancelIndicator) {
        Objects.requireNonNull(channel);
        this.channel = channel;
        this.cancelIndicator = cancelIndicator;
    }

    public void read(ByteBuffer ioBuffer, long filePos, CompletionHandler<Void, Void> handler) {
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = false;
        this.flush = false;
        this.flushMeta = false;
        exec(handler, filePos);
    }

    public void write(ByteBuffer ioBuffer, long filePos, CompletionHandler<Void, Void> handler) {
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = true;
        this.flush = false;
        this.flushMeta = false;
        exec(handler, filePos);
    }

    public void writeAndFlush(ByteBuffer ioBuffer, long filePos, boolean flushMeta,
                              CompletionHandler<Void, Void> handler) {
        this.ioBuffer = ioBuffer;
        this.filePos = filePos;
        this.position = ioBuffer.position();
        this.write = true;
        this.flush = true;
        this.flushMeta = flushMeta;
        exec(handler, filePos);
    }

    public void retry(CompletionHandler<Void, Void> handler) {
        ioBuffer.position(position);
        exec(handler, filePos);
    }

    private void exec(CompletionHandler<Void, Void> handler, long pos) {
        try {
            if (write) {
                channel.write(ioBuffer, pos, handler, this);
            } else {
                channel.read(ioBuffer, pos, handler, this);
            }
        } catch (Throwable e) {
            handler.failed(e, null);
        }
    }

    @Override
    public void completed(Integer result, CompletionHandler<Void, Void> handler) {
        if (result < 0) {
            handler.failed(new RaftException("read end of file"), null);
            return;
        }
        if (ioBuffer.hasRemaining()) {
            if (cancelIndicator != null && cancelIndicator.get()) {
                handler.failed(new FiberCancelException(), null);
                return;
            }
            int bytes = ioBuffer.position() - position;
            exec(handler, filePos + bytes);
        } else {
            try {
                if (flush) {
                    channel.force(flushMeta);
                }
                handler.completed(null, null);
            } catch (Throwable e) {
                handler.failed(e, null);
            }
        }
    }

    @Override
    public void failed(Throwable exc, CompletionHandler<Void, Void> handler) {
        handler.failed(exc, null);
    }

    public static CompletionHandler<Void, Void> wrap(FiberFuture<Void> f) {
        return new CompletionHandler<>() {
            @Override
            public void completed(Void result, Void attachment) {
                f.complete(null);
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                f.completeExceptionally(exc);
            }
        };
    }
}

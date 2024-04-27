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
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.Executor;

/**
 * @author huangli
 */
public class ForceFrame extends FiberFrame<Void> {

    private static final DtLog log = DtLogs.getLogger(ForceFrame.class);

    private final AsynchronousFileChannel channel;
    private final Executor ioExecutor;
    private final boolean meta;

    public ForceFrame(AsynchronousFileChannel channel, Executor ioExecutor, boolean meta) {
        this.channel = channel;
        this.ioExecutor = ioExecutor;
        this.meta = meta;
    }

    @Override
    public final FrameCallResult execute(Void input) throws Throwable {
        FiberFuture<Void> f = getFiberGroup().newFuture("forceFile");
        ioExecutor.execute(() -> {
            try {
                channel.force(meta);
                f.fireComplete(null);
            } catch (Throwable e) {
                log.error("force file failed: {}", channel);
                f.fireCompleteExceptionally(e);
            }
        });
        return f.await(this::afterForce);
    }

    protected FrameCallResult afterForce(Void v) {
        return Fiber.frameReturn();
    }
}

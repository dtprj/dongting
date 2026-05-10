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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.fiber.FiberGroup;

import java.io.File;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * @author huangli
 */
public class DtFile extends AbstractFile<AsynchronousFileChannel> {
    AsynchronousFileChannel channel;

    public DtFile(File file, FiberGroup fiberGroup, Set<OpenOption> openOptions,
                  ExecutorService ioExecutor) {
        super(file, fiberGroup, openOptions, ioExecutor);
    }

    @Override
    public boolean isOpen() {
        return channel != null;
    }

    @Override
    protected AsynchronousFileChannel doSyncOpen() throws IOException {
        return AsynchronousFileChannel.open(file.toPath(), openOptions, ioExecutor);
    }

    @Override
    protected void afterSyncOpen(AsynchronousFileChannel openResult) {
        channel = openResult;
    }

    @Override
    protected void doClose() {
        DtUtil.close(channel);
        channel = null;
    }

    @Override
    public void doForce(boolean meta) throws IOException {
        channel.force(meta);
    }

    @Override
    protected void dropOpenResult(AsynchronousFileChannel o) {
        if (o != null) {
            DtUtil.close(o);
        }
    }
}

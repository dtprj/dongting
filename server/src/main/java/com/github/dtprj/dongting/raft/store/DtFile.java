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
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.log.BugLog;

import java.io.File;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * @author huangli
 */
public class DtFile {
    private final File file;
    private AsynchronousFileChannel channel;
    private final Set<OpenOption> openOptions;
    final ExecutorService ioExecutor;
    final FiberGroup fiberGroup;

    private int readers;
    private int writers;

    private final FiberCondition noRwCond;

    public DtFile(File file, FiberGroup fiberGroup, Set<OpenOption> openOptions,
                  ExecutorService ioExecutor) {
        this.file = file;
        this.openOptions = openOptions;
        this.ioExecutor = ioExecutor;
        this.fiberGroup = fiberGroup;
        this.noRwCond = fiberGroup.newCondition("noRw-" + file.getName());
    }

    public File getFile() {
        return file;
    }

    public AsynchronousFileChannel getChannel() {
        return channel;
    }

    public boolean isOpen() {
        return channel != null;
    }

    public void open() throws IOException {
        if (channel != null) {
            return;
        }
        channel = AsynchronousFileChannel.open(file.toPath(), openOptions, ioExecutor);
    }

    public void close() {
        if (channel == null) {
            return;
        }
        if (inUse()) {
            BugLog.log(new IllegalStateException("close file while in use: " + file.getPath()));
        }
        DtUtil.close(channel);
        channel = null;
    }

    public FiberCondition getNoRwCond() {
        return noRwCond;
    }

    public void incReaders() {
        readers++;
    }

    public void decReaders() {
        readers--;
        if (readers <= 0 && writers <= 0) {
            noRwCond.signalAll();
        }
    }

    public int getReaders() {
        return readers;
    }

    public void incWriters() {
        writers++;
    }

    public void decWriters() {
        writers--;
        if (readers <= 0 && writers <= 0) {
            noRwCond.signalAll();
        }
    }

    public int getWriters() {
        return writers;
    }

    public boolean inUse() {
        return readers > 0 || writers > 0;
    }
}

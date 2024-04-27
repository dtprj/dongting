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

import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FiberLock;

import java.io.File;
import java.nio.channels.AsynchronousFileChannel;

/**
 * @author huangli
 */
public class DtFile {
    private final File file;
    private final AsynchronousFileChannel channel;

    private final FiberLock lock;

    public DtFile(File file, AsynchronousFileChannel channel, FiberGroup fiberGroup) {
        this.file = file;
        this.channel = channel;
        this.lock = fiberGroup.newLock("fileLock");
    }

    public File getFile() {
        return file;
    }

    public FiberLock getLock() {
        return lock;
    }

    public AsynchronousFileChannel getChannel() {
        return channel;
    }
}

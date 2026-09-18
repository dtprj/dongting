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

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * @author huangli
 */
public class MainLogFile extends QueueFile {
    long firstTimestamp;
    long firstIndex;
    int firstTerm;

    // serializes non-positional gathering writes since they mutate the channel position,
    // see AsyncIoTask
    final ReentrantLock gatheringWriteLock = new ReentrantLock();

    public MainLogFile(long startPos, long endPos, File file, FiberGroup group,
                       ExecutorService ioExecutor,
                       Consumer<QueueFile> accessCallback, long currentTimeMillis) {
        super(startPos, endPos, file, group, ioExecutor, accessCallback, currentTimeMillis);
    }
}

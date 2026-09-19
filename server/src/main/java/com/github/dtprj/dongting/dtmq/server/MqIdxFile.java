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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.raft.store.QueueFile;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * @author huangli
 */
final class MqIdxFile extends QueueFile {

    // pos of the last item of this file; -1 unknown until the file is sealed by a
    // flush, or read back by cleanup
    long lastItemPos = -1;

    MqIdxFile(long startPos, long endPos, File file, FiberGroup group,
              ExecutorService ioExecutor, Consumer<QueueFile> accessCallback, long currentTimeMillis) {
        super(startPos, endPos, file, group, ioExecutor, accessCallback, currentTimeMillis);
    }
}

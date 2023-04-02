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
package com.github.dtprj.dongting.raft.file;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author huangli
 */
class LogFile {
    final long startIndex;
    final FileChannel channel;
    final long endIndex;
    boolean finished;
    int writePosition;
    long lastUpdateMillis;

    public LogFile(long startIndex, long endIndex, FileChannel channel) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.channel = channel;
    }
}

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
package com.github.dtprj.dongting.raft.sm;

import com.github.dtprj.dongting.buf.RefBuffer;

import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class DefaultSnapshot extends Snapshot {
    private final AsynchronousFileChannel channel;

    public DefaultSnapshot(long lastIncludedIndex, int lastIncludedTerm, AsynchronousFileChannel channel) {
        super(lastIncludedIndex, lastIncludedTerm);
        this.channel = channel;
    }

    @Override
    public CompletableFuture<RefBuffer> readNext() {
        return null;
    }

    @Override
    protected void doClose() {
    }
}

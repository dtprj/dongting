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

/**
 * @author huangli
 */
@FunctionalInterface
public interface IoCallback {
    /**
     * Execute mmap read operation using the mapped byte buffer.
     * This method runs in the io thread pool, NOT the raft fiber thread.
     * @param mmapBuffer a duplicate of the DtFile's MappedByteBuffer (READ_ONLY).
     *                   Each io thread gets its own duplicate with independent position/limit.
     *                   The callback sets position/limit as needed before reading.
     * @throws IOException if IO error occurs (will trigger retry if configured)
     * @apiNote This callback may be invoked multiple times when retries are configured.
     * Implementations must be retry-safe: restore any mutable state before each invocation.
     */
    void run(ByteBuffer mmapBuffer) throws IOException;
}

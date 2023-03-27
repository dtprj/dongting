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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.common.Pair;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * @author huangli
 */
public interface StateMachine extends AutoCloseable {
    void init(RaftLog raftLog);

    Object decode(ByteBuffer logData);

    Object exec(long index, Object input);

    void installSnapshot(boolean start, boolean finish, ByteBuffer data);

    Snapshot findLatestSnapshot();

    Pair<Snapshot, Iterator<ByteBuffer>> openLatestSnapshotIterator();

    void closeIterator(Iterator<ByteBuffer> iterator);
}

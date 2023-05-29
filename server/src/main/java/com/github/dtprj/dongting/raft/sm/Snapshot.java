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

/**
 * @author huangli
 */
public abstract class Snapshot {
    private final long lastIncludedIndex;
    private final int lastIncludedTerm;

    protected Snapshot(int lastIncludedTerm, long lastIncludedIndex) {
        this.lastIncludedTerm = lastIncludedTerm;
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public abstract SnapshotIterator openIterator();

}

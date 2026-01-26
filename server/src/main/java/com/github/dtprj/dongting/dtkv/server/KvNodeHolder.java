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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.common.ByteArray;

/**
 * @author huangli
 */
final class KvNodeHolder {
    final ByteArray key;
    final ByteArray keyInDir;
    final KvNodeHolder parent;

    KvNodeEx latest;

    WatchHolder watchHolder;

    long updateIndex;
    boolean inUpdateQueue;

    int childHolderCount;

    // Doubly-linked list pointers for O(1) removal from KvMap
    KvNodeHolder prev;
    KvNodeHolder next;

    public KvNodeHolder(ByteArray key, ByteArray keyInDir, KvNodeEx n, KvNodeHolder parent) {
        this.key = key;
        this.keyInDir = keyInDir;
        this.latest = n;
        this.parent = parent;
    }
}

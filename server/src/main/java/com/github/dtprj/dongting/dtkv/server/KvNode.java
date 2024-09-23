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

import java.util.HashMap;

/**
 * @author huangli
 */
class KvNode {
    final byte[] data;
    final long raftIndex;
    final boolean dir;
    final HashMap<String, KvNodeHolder> children;

    KvNode previous;
    long removeAtIndex;

    public KvNode(long raftIndex, byte[] data) {
        this.raftIndex = raftIndex;
        this.data = data;
        this.dir = false;
        children = null;
    }

    public KvNode() {
        this.raftIndex = 0;
        this.data = null;
        this.dir = true;
        this.children = new HashMap<>();
    }
}

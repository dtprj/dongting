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
import com.github.dtprj.dongting.dtkv.KvNode;

import java.util.HashMap;

/**
 * @author huangli
 */
final class KvNodeEx extends KvNode {
    private static final HashMap<ByteArray, KvNodeHolder> EMPTY_MAP = new HashMap<>(0);

    final HashMap<ByteArray, KvNodeHolder> children;
    final boolean removed;

    KvNodeEx previous;

    TtlInfo ttlInfo;

    public KvNodeEx(long createIndex, long createTime, long updateIndex, long updateTime, boolean dir, byte[] data) {
        super(createIndex, createTime, updateIndex, updateTime, dir, data);
        this.removed = false;
        if (dir) {
            children = new HashMap<>();
        } else {
            children = null;
        }
    }

    public KvNodeEx(long createIndex, long createTime, long updateIndex, long updateTime, boolean dir) {
        super(createIndex, createTime, updateIndex, updateTime, dir, null);
        this.removed = true;
        if (dir) {
            children = EMPTY_MAP;
        } else {
            children = null;
        }
    }

    public KvNodeEx(KvNodeEx old, long updateIndex, long updateTime, byte[] newData) {
        super(old.createIndex, old.createTime, updateIndex, updateTime, old.isDir, newData);
        this.children = old.children;
        this.removed = false;
        if (old.ttlInfo != null) {
            this.ttlInfo = old.ttlInfo;
            this.ownerUuid = old.ownerUuid;
            this.ttlMillis = old.ttlMillis;
            this.expireTime = old.expireTime;
        }
    }
}

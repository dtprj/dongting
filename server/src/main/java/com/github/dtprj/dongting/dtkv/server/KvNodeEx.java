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
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.dtkv.KvResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;

/**
 * @author huangli
 */
final class KvNodeEx extends KvNode {

    private final HashMap<ByteArray, KvNodeHolder> children;
    private final TreeSet<KvNodeHolder> lockOrderQueue;
    final boolean removed;

    KvNodeEx previous;

    TtlInfo ttlInfo;

    public KvNodeEx(long createIndex, long createTime, long updateIndex, long updateTime, int flag, byte[] data) {
        super(createIndex, createTime, updateIndex, updateTime, flag, data);
        this.removed = false;
        if ((flag & KvNode.FLAG_DIR_MASK) != 0) {
            children = new HashMap<>();
            if ((flag & KvNode.FLAG_LOCK_MASK) != 0) {
                lockOrderQueue = new TreeSet<>(Comparator.comparingLong(a -> a.latest.createIndex));
            } else {
                lockOrderQueue = null;
            }
        } else {
            children = null;
            lockOrderQueue = null;
        }
    }

    // remove shadow
    public KvNodeEx(long createIndex, long createTime, long updateIndex, long updateTime) {
        super(createIndex, createTime, updateIndex, updateTime, 0, null);
        this.removed = true;
        this.children = null;
        this.lockOrderQueue = null;
    }

    public KvNodeEx(KvNodeEx old, long updateIndex, long updateTime, byte[] newData) {
        super(old.createIndex, old.createTime, updateIndex, updateTime, old.flag, newData);
        this.children = old.children;
        this.lockOrderQueue = old.lockOrderQueue;
        this.removed = false;
        this.ttlInfo = old.ttlInfo;
    }

    ArrayList<KvResult> list() {
        ArrayList<KvResult> list = new ArrayList<>(children.size());
        for (KvNodeHolder child : children.values()) {
            list.add(new KvResult(KvCodes.SUCCESS, child.latest, child.keyInDir));
        }
        return list;
    }

    void addChild(KvNodeHolder c) {
        children.put(c.keyInDir, c);
        if ((flag & KvNode.FLAG_LOCK_MASK) != 0) {
            lockOrderQueue.add(c);
        }
    }

    int childCount() {
        return children == null ? 0 : children.size();
    }

    void removeChild(ByteArray keyInDir) {
        KvNodeHolder h = children.remove(keyInDir);
        if ((flag & KvNode.FLAG_LOCK_MASK) != 0 && h != null) {
            lockOrderQueue.remove(h);
        }
    }

    KvNodeHolder peekNext() {
        return lockOrderQueue.first();
    }

    Collection<KvNodeHolder> childrenValues() {
        return children.values();
    }
}

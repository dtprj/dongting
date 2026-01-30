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

import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.log.BugLog;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * @author huangli
 */
final class KvNodeEx extends KvNode {

    // Sentinel head node for children doubly-linked list. next points to first child, prev points to last child.
    // The sentinel itself is not a real child node.
    final KvNodeHolder children;
    // Child count for O(1) access (avoid traversing the list)
    private int childCount;

    private final TreeSet<KvNodeHolder> lockOrderQueue;
    final boolean removed;

    KvNodeEx previous;

    TtlInfo ttlInfo;

    public KvNodeEx(long createIndex, long createTime, long updateIndex, long updateTime, int flag, byte[] data) {
        super(createIndex, createTime, updateIndex, updateTime, flag, data);
        this.removed = false;
        if ((flag & KvNode.FLAG_DIR_MASK) != 0) {
            // Create sentinel head node for children doubly-linked list
            this.children = new KvNodeHolder(null, null, null, null);
            this.children.childPrev = this.children;
            this.children.childNext = this.children;
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
        this.childCount = old.childCount;
    }

    ArrayList<KvResult> list() {
        ArrayList<KvResult> list = new ArrayList<>(childCount);
        KvNodeHolder sentinel = children;
        for (KvNodeHolder child = sentinel.childNext; child != sentinel; child = child.childNext) {
            list.add(new KvResult(KvCodes.SUCCESS, child.latest, child.keyInDir));
        }
        return list;
    }

    void addChild(KvNodeHolder c) {
        if (c.childNext != null || c.childPrev != null) {
            BugLog.logAndThrow("already added");
        }
        // Insert at the end of the doubly-linked list (before sentinel)
        KvNodeHolder sentinel = children;
        c.childPrev = sentinel.childPrev;
        c.childNext = sentinel;
        sentinel.childPrev.childNext = c;
        sentinel.childPrev = c;
        childCount++;
        if ((flag & KvNode.FLAG_LOCK_MASK) != 0) {
            int childFlag = c.latest.flag;
            if ((childFlag & KvNode.FLAG_LOCK_MASK) == 0) {
                BugLog.logAndThrow("child has no lock mask");
            }
            if ((childFlag & KvNode.FLAG_DIR_MASK) != 0) {
                BugLog.logAndThrow("child is dir");
            }
            lockOrderQueue.add(c);
        }
    }

    int childCount() {
        return childCount;
    }

    void removeChild(KvNodeHolder h) {
        // O(1) removal from doubly-linked list
        h.childPrev.childNext = h.childNext;
        h.childNext.childPrev = h.childPrev;
        h.childPrev = null;
        h.childNext = null;
        childCount--;
        if ((flag & KvNode.FLAG_LOCK_MASK) != 0) {
            int childFlag = h.latest.flag;
            if ((childFlag & KvNode.FLAG_LOCK_MASK) == 0) {
                BugLog.logAndThrow("child has no lock mask");
            }
            if ((childFlag & KvNode.FLAG_DIR_MASK) != 0) {
                BugLog.logAndThrow("child is dir");
            }
            if (!lockOrderQueue.remove(h)) {
                BugLog.logAndThrow("lockOrderQueue remove fail");
            }
        }
    }

    KvNodeHolder peekNextOwner() {
        TreeSet<KvNodeHolder> q = lockOrderQueue;
        if (q == null) {
            BugLog.logAndThrow("lockOrderQueue is null");
        }
        //noinspection DataFlowIssue
        return q.isEmpty() ? null : q.first();
    }

    Iterable<KvNodeHolder> childrenValues() {
        return () -> new Iterator<>() {
            private KvNodeHolder current = children.childNext;
            private final KvNodeHolder sentinel = children;

            @Override
            public boolean hasNext() {
                return current != sentinel;
            }

            @Override
            public KvNodeHolder next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                KvNodeHolder result = current;
                current = current.childNext;
                return result;
            }
        };
    }
}

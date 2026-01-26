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

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HashMap + doubly-linked list implementation for better performance.
 * Thread safety is provided by StampedLock in KvImpl, so no concurrent data structures needed.
 *
 * @author huangli
 */
class KvMap {
    private final HashMap<ByteArray, KvNodeHolder> map;

    // Sentinel head node, next point to the first node, prev point to the last node
    private final KvNodeHolder head;

    public KvMap(int initCapacity, float loadFactor) {
        this.map = new HashMap<>(initCapacity, loadFactor);
        this.head = new KvNodeHolder(null, null, null, null);
        this.head.prev = this.head;
        this.head.next = this.head;
    }

    public KvNodeHolder get(ByteArray key) {
        return map.get(key);
    }

    public KvNodeHolder put(ByteArray key, KvNodeHolder value) {
        KvNodeHolder old = map.put(key, value);
        if (old != null) {
            removeFromList(old);
        }
        addToList(value);
        return old;
    }

    public KvNodeHolder remove(ByteArray key) {
        KvNodeHolder old = map.remove(key);
        if (old != null) {
            removeFromList(old);
        }
        return old;
    }

    public int size() {
        return map.size();
    }

    public Iterator<KvNodeHolder> iterator() {
        return new KvMapIterator();
    }

    private void addToList(KvNodeHolder node) {
        node.prev = head.prev;
        node.next = head;
        head.prev.next = node;
        head.prev = node;
    }

    private void removeFromList(KvNodeHolder node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
    }

    private class KvMapIterator implements Iterator<KvNodeHolder> {
        private KvNodeHolder current = head.next;

        @Override
        public boolean hasNext() {
            return current != head;
        }

        @Override
        public KvNodeHolder next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            KvNodeHolder result = current;
            current = current.next;
            return result;
        }
    }
}

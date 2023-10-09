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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.raft.store.IndexedQueue;

/**
 * @author huangli
 */
public class RaftCondition {
    private final IndexedQueue<Runnable> queue = new IndexedQueue<>(16);
    private boolean condition;

    public RaftCondition(boolean condition) {
        this.condition = condition;
    }

    public void signal() {
        condition = true;
        while (condition) {
            Runnable t = queue.removeFirst();
            t.run();
        }
    }

    public void waitAtLast(Runnable task) {
        queue.addLast(task);
    }

    public void waitAtFirst(Runnable task) {
        queue.addFirst(task);
    }

    public void clear() {
        while (queue.removeFirst() != null) ;
    }

    public boolean isFalse() {
        return !condition;
    }

    public void setFalse() {
        this.condition = false;
    }
}

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
package com.github.dtprj.dongting.java8;

import com.github.dtprj.dongting.queue.LinkedNode;
import com.github.dtprj.dongting.queue.MpscLinkedQueue;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author huangli
 */
public class Java8MpscLinkedQueue<E> extends MpscLinkedQueue<E> {

    private volatile boolean shutdown;
    private volatile LinkedNode<E> tail;

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<Java8MpscLinkedQueue, LinkedNode> PRODUCER_NODE;

    static {
        PRODUCER_NODE = AtomicReferenceFieldUpdater.newUpdater(Java8MpscLinkedQueue.class, LinkedNode.class, "tail");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected LinkedNode<E> getAndSetTailRelease(LinkedNode<E> nextNode) {
        return (LinkedNode<E>) PRODUCER_NODE.getAndSet(this, nextNode);
    }

    @Override
    protected void initTailVolatile(LinkedNode<E> node) {
        tail = node;
    }

    @Override
    protected boolean isShutdownVolatile() {
        return shutdown;
    }

    @Override
    protected void markShutdownVolatile() {
        shutdown = true;
    }
}

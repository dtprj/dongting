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
import com.github.dtprj.dongting.queue.LinkedQueueProducerRef;
import com.github.dtprj.dongting.queue.MpscLinkedQueue;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author huangli
 */
public class Java8MpscLinkedQueue<E> extends MpscLinkedQueue<E> {


    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<LinkedQueueProducerRef, LinkedNode> PRODUCER_NODE;
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<LinkedNode, LinkedNode> NEXT;

    static {
        PRODUCER_NODE = AtomicReferenceFieldUpdater.newUpdater(LinkedQueueProducerRef.class, LinkedNode.class, "tail");
        NEXT = AtomicReferenceFieldUpdater.newUpdater(LinkedNode.class, LinkedNode.class, "next");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected LinkedNode<E> getAndSetTailRelease(LinkedNode<E> nextNode) {
        return (LinkedNode<E>) PRODUCER_NODE.getAndSet(this, nextNode);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected LinkedNode<E> getNextAcquire(LinkedNode<E> node) {
        return (LinkedNode<E>) NEXT.get(node);
    }

    @Override
    protected void setNextRelease(LinkedNode<E> node, LinkedNode<E> nextNode) {
        NEXT.lazySet(node, nextNode);
    }
}

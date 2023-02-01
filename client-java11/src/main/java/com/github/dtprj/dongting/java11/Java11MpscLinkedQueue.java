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
package com.github.dtprj.dongting.java11;

import com.github.dtprj.dongting.queue.LinkedNode;
import com.github.dtprj.dongting.queue.MpscLinkedQueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * @author huangli
 */
public class Java11MpscLinkedQueue<E> extends MpscLinkedQueue<E> {
    private static final VarHandle PRODUCER_NODE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            PRODUCER_NODE = l.findVarHandle(MpscLinkedQueue.class, "producerNode", LinkedNode.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    @Override
    protected LinkedNode<E> newNode(E value) {
        return new Java11LinkedNode<>(value);
    }

    @Override
    protected LinkedNode<E> getAndSetProducerNodeRelease(LinkedNode<E> nextNode) {
        //noinspection unchecked
        return (LinkedNode<E>) PRODUCER_NODE.getAndSetRelease(this, nextNode);
    }
}

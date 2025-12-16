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
import com.github.dtprj.dongting.unsafe11.MpscLinkedQueueProducerRef;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * @author huangli
 */
public class Java11MpscLinkedQueue<E> extends MpscLinkedQueue<E> {

    private MpscLinkedQueueProducerRef producerRef;
    private static final VarHandle TAIL;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            TAIL = l.findVarHandle(MpscLinkedQueueProducerRef.class, "tail", Object.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected LinkedNode<E> getAndSetTailRelease(LinkedNode<E> nextNode) {
        return (LinkedNode<E>) TAIL.getAndSetRelease(producerRef, nextNode);
    }

    @Override
    protected void initTailVolatile(LinkedNode<E> node) {
        producerRef = new MpscLinkedQueueProducerRef();
        producerRef.tail = node;
    }

    @Override
    protected boolean isShutdownVolatile() {
        return producerRef.shutdown;
    }

    @Override
    protected void markShutdownVolatile() {
        producerRef.shutdown = true;
    }


}

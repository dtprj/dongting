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
package com.github.dtprj.dongting.queue;

import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.VersionFactory;

import java.util.Objects;

/**
 * @author huangli
 */
@SuppressWarnings({"unused"})
public abstract class MpscLinkedQueue<E> {
    @SuppressWarnings("rawtypes")
    private static final LinkedNode SHUTDOWN_NODE = newInstance().newNode(null);

    // 128 bytes padding to avoid false share
    long p00, p01, p02, p03, p04, p05, p06, p07, p08, p09, p0a, p0b, p0c, p0d, p0e, p0f;

    protected boolean shutdown;
    protected volatile LinkedNode<E> tail;

    long p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p1a, p1b, p1c, p1d, p1e, p1f;

    protected LinkedNode<E> head;

    long p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p2a, p2b, p2c, p2d, p2e, p2f;

    protected MpscLinkedQueue() {
        LinkedNode<E> node = newNode(null);
        head = node;
        tail = node;
    }

    public static <E> MpscLinkedQueue<E> newInstance() {
        return VersionFactory.getInstance().newMpscLinkedQueue();
    }

    public E relaxedPoll() {
        LinkedNode<E> next = head.getNextAcquire();
        if (next == null) {
            return null;
        }
        if (next != SHUTDOWN_NODE) {
            E value = next.getValue();
            next.setValue(null); // just mark
            head = next;
            return value;
        }
        //noinspection unchecked
        head = SHUTDOWN_NODE;
        return null;
    }

    public boolean offer(E value) {
        Objects.requireNonNull(value);
        // set plain
        LinkedNode<E> newTail = newNode(value);
        offer0(newTail);
        return true;
    }

    private void offer0(LinkedNode<E> newTail) {
        LinkedNode<E> oldTail = getAndSetTail(newTail);
        if (shutdown && newTail != SHUTDOWN_NODE) {
            // don't keep reference of newProduceNode
            //noinspection unchecked
            tail = SHUTDOWN_NODE;

            throw new DtException("queue is shutdown");
        }
        // so consumer can read value
        oldTail.setNextRelease(newTail);
    }

    protected abstract LinkedNode<E> getAndSetTail(LinkedNode<E> nextNode);

    protected abstract LinkedNode<E> newNode(E value);

    @SuppressWarnings("unchecked")
    public void shutdown() {
        shutdown = true;
        offer0(SHUTDOWN_NODE);
    }
}



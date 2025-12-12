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

import com.github.dtprj.dongting.common.VersionFactory;

import java.util.Objects;
import java.util.concurrent.locks.LockSupport;

/**
 * @author huangli
 */
@SuppressWarnings({"unused"})
public abstract class MpscLinkedQueue<E> {
    private static final VersionFactory VERSION_FACTORY = VersionFactory.getInstance();
    @SuppressWarnings("rawtypes")
    private static final LinkedNode SHUTDOWN_NODE = VERSION_FACTORY.newNode(null);

    // 128 bytes padding to avoid false share
    long p00, p01, p02, p03, p04, p05, p06, p07, p08, p09, p0a, p0b, p0c, p0d, p0e, p0f;

    private volatile boolean shutdown;
    protected volatile LinkedNode<E> tail;

    long p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p1a, p1b, p1c, p1d, p1e, p1f;

    protected LinkedNode<E> head;

    long p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p2a, p2b, p2c, p2d, p2e, p2f;

    protected MpscLinkedQueue() {
        LinkedNode<E> node = VERSION_FACTORY.newNode(null);
        head = node;
        tail = node;
        VERSION_FACTORY.releaseFence();
    }

    public static <E> MpscLinkedQueue<E> newInstance() {
        return VERSION_FACTORY.newMpscLinkedQueue();
    }

    public E relaxedPoll() {
        // no need to check SHUTDOWN_NODE
        LinkedNode<E> next = head.getNextAcquire();
        if (next == null) {
            return null;
        }
        E value = next.getValue();
        next.setValue(null); // just mark
        head = next;
        return value;
    }

    public E poll() {
        LinkedNode<E> next = head.getNextAcquire();
        if (next == null) {
            if (head != SHUTDOWN_NODE && head != tail) { // tail is volatile read
                for (int i = 0; ; i++) {
                    next = head.getNextAcquire();
                    if (next != null) {
                        break;
                    }
                    spin(i);
                }
            } else {
                return null;
            }
        }
        E value = next.getValue();
        next.setValue(null); // just mark
        head = next;
        return value;
    }

    public boolean offer(E value) {
        Objects.requireNonNull(value);
        // set plain
        LinkedNode<E> newTail = VERSION_FACTORY.newNode(value);
        return offer0(newTail);
    }

    private boolean offer0(LinkedNode<E> newTail) {
        // need getAndSetProducerNodePlain, but no such method
        LinkedNode<E> oldTail = getAndSetTailRelease(newTail);
        if (shutdown) { // volatile read
            if (newTail == SHUTDOWN_NODE) {
                // forward propagation (init)
                oldTail.shutdownStatus = LinkedNode.SHUTDOWN_STATUS_BEFORE;

                // here not return, go down to set nextRelease and return true
            } else {
                // We need decide return true or false, based on the LinkedNode is before or after SHUTDOWN_NODE.
                // If before, return true and link it in the queue, consumer can read it.
                // If after, return false and don't link it in the queue, consumer can't read it
                if (oldTail == SHUTDOWN_NODE) {
                    // backward propagation (init)
                    newTail.shutdownStatus = LinkedNode.SHUTDOWN_STATUS_AFTER;
                } else {
                    for (int i = 0; ; i++) {
                        if (oldTail.shutdownStatus == LinkedNode.SHUTDOWN_STATUS_AFTER) {
                            // backward propagation
                            newTail.shutdownStatus = LinkedNode.SHUTDOWN_STATUS_AFTER;
                            break;
                        }
                        if (newTail.shutdownStatus == LinkedNode.SHUTDOWN_STATUS_BEFORE) {
                            // forward propagation
                            oldTail.shutdownStatus = LinkedNode.SHUTDOWN_STATUS_BEFORE;
                            break;
                        }
                        spin(i);
                    }
                }

                // don't keep reference
                newTail.setValue(null);

                //noinspection StatementWithEmptyBody
                if (newTail.shutdownStatus == LinkedNode.SHUTDOWN_STATUS_AFTER) {
                    return false;
                } else {
                    // go down to set nextRelease and return true
                }
            }
        }
        // so consumer can read value
        oldTail.setNextRelease(newTail);
        return true;
    }

    protected abstract LinkedNode<E> getAndSetTailRelease(LinkedNode<E> nextNode);

    /**
     * The complex shutdown logic in offer0 method ensures shutdown behavior is graceful and strict.
     * All the elements offered before SHUTDOWN_NODE will return true and be visible to the consumer.
     * All the elements offered after SHUTDOWN_NODE will return false and won't be visible to the consumer.
     * After shutdown is called, the consumer should invoke poll() until it returns null, and then exit safely,
     * no elements can be added to the queue successfully and not be consumed.
     */
    @SuppressWarnings("unchecked")
    public void shutdown() {
        boolean doShutdown = false;
        synchronized (this) {
            if (!shutdown) {
                shutdown = true;
                doShutdown = true;
            }
        }
        if (doShutdown) {
            offer0(SHUTDOWN_NODE);
        }
    }

    private void spin(int i) {
        if (i >= 0 && i < 100) {
            VERSION_FACTORY.onSpinWait();
        } else if (i < 300) {
            Thread.yield();
        } else if (i < 2000) {
            LockSupport.parkNanos(5000);
        } else {
            LockSupport.parkNanos(10_000);
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }
}



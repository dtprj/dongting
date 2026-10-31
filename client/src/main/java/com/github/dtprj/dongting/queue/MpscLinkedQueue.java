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
public abstract class MpscLinkedQueue<E> extends LinkedQueuePadding1<E> {
    private static final VersionFactory VERSION_FACTORY = VersionFactory.getInstance();
    @SuppressWarnings("rawtypes")
    private static final LinkedNode SHUTDOWN_NODE = new LinkedNode<>(null);
    protected LinkedNode<E> head;

    protected MpscLinkedQueue() {
        LinkedNode<E> node = new LinkedNode<>(null);
        head = node;
        tail = node;
    }

    public static <E> MpscLinkedQueue<E> newInstance() {
        return VERSION_FACTORY.newMpscLinkedQueue();
    }

    public E relaxedPoll() {
        // no need to check SHUTDOWN_NODE
        LinkedNode<E> next = getNextAcquire(head);
        if (next == null) {
            return null;
        }
        E value = next.getValue();
        next.setValue(null); // just mark
        head = next;
        return value;
    }

    public boolean offer(E value) {
        Objects.requireNonNull(value);
        // set plain
        LinkedNode<E> newTail = new LinkedNode<>(value);
        return offer0(newTail);
    }

    private boolean offer0(LinkedNode<E> newTail) {
        LinkedNode<E> oldTail = getAndSetTail(newTail);
        if (shutdown) { // plain read
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
                    FIND_SHUTDOWN_STATUS:
                    for (int i = 0; ; i++) {
                        if (oldTail.shutdownStatus == LinkedNode.SHUTDOWN_STATUS_AFTER) {
                            // backward propagation
                            newTail.shutdownStatus = LinkedNode.SHUTDOWN_STATUS_AFTER;
                            break;
                        }
                        // forward propagation may be broken since some operation may read shutdown == false
                        // and exit early, so we walk the next chain
                        LinkedNode<E> next = getNextAcquire(newTail);
                        while (next != null) {
                            if (next.shutdownStatus == LinkedNode.SHUTDOWN_STATUS_BEFORE || next == SHUTDOWN_NODE) {
                                newTail.shutdownStatus = LinkedNode.SHUTDOWN_STATUS_BEFORE;
                                break FIND_SHUTDOWN_STATUS;
                            }
                            next = getNextAcquire(next);
                        }
                        spin(i);
                    }
                }

                //noinspection StatementWithEmptyBody
                if (newTail.shutdownStatus == LinkedNode.SHUTDOWN_STATUS_AFTER) {
                    // don't keep reference
                    newTail.setValue(null);
                    return false;
                } else {
                    // go down to set nextRelease and return true
                }
            }
        }
        // so consumer can read value
        setNextRelease(oldTail, newTail);
        return true;
    }

    protected abstract LinkedNode<E> getAndSetTail(LinkedNode<E> nextNode);

    /**
     * The complex shutdown logic in offer0 method ensures shutdown behavior is graceful and strict.
     * All the elements offered before SHUTDOWN_NODE will return true and be visible to the consumer.
     * All the elements offered after SHUTDOWN_NODE will return false and won't be visible to the consumer.
     * After shutdown is called, the consumer should invoke relaxedPoll() until isConsumeFinished() returns true,
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

    public boolean isConsumeFinished() {
        return head == SHUTDOWN_NODE;
    }

    protected abstract LinkedNode<E> getNextAcquire(LinkedNode<E> node);

    protected abstract void setNextRelease(LinkedNode<E> node, LinkedNode<E> nextNode);
}



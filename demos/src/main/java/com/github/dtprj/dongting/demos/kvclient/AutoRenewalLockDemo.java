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
package com.github.dtprj.dongting.demos.kvclient;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtkv.AutoRenewalLock;
import com.github.dtprj.dongting.dtkv.AutoRenewalLockListener;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvNode;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Auto-renewal lock demo with counter increment.
 *
 * <p>This demo demonstrates:
 * <ul>
 *   <li>Auto-renewal lock acquisition and background renewal</li>
 *   <li>Counter increment every second when holding the lock</li>
 *   <li>Lock release and acquisition when process exits</li>
 * </ul>
 *
 * <p>Run two clients to test:
 * <ol>
 *   <li>Start server: ./bin/start-dongting.sh</li>
 *   <li>Run this demo in two terminals</li>
 *   <li>Exit first instance with Ctrl+C, observe second instance acquiring the lock immediately</li>
 * </ol>
 *
 * @author huangli
 */
public class AutoRenewalLockDemo {

    private static final int GROUP_ID = 0;
    private static final String SERVERS = "1,127.0.0.1:9332";
    private static final int[] MEMBERS = {1};

    private static final String DIR_NAME = "auto_lock_dir";
    private static final String COUNTER_KEY = "auto_lock_dir.counter";
    private static final String LOCK_NAME = "auto_lock_dir.lock";
    private static final long LEASE_MILLIS = 30000;
    private static final long UPDATE_INTERVAL = 1000;

    private static volatile boolean running = true;
    private static final ReentrantLock reentrantLock = new ReentrantLock();
    private static final Condition lockEventCondition = reentrantLock.newCondition();
    private static Thread counterThread;

    public static void main(String[] args) throws Exception {
        KvClient kvClient = new KvClient();
        kvClient.start();

        kvClient.getRaftClient().clientAddNode(SERVERS);
        kvClient.getRaftClient().clientAddOrUpdateGroup(GROUP_ID, MEMBERS);

        // idempotent
        kvClient.mkdir(GROUP_ID, DIR_NAME.getBytes());

        AutoRenewalLockListener listener = new AutoRenewalLockListener() {
            @Override
            public void onAcquired(AutoRenewalLock lock) {
                System.out.println("[Lock acquired]");
                signalEvent();
            }

            @Override
            public void onLost(AutoRenewalLock lock) {
                System.out.println("[Lock lost]");
                signalEvent();
            }
        };

        AutoRenewalLock autoLock = kvClient.createAutoRenewalLock(GROUP_ID, LOCK_NAME.getBytes(), LEASE_MILLIS, listener);

        counterThread = new Thread(() -> runUpdater(kvClient, autoLock));
        counterThread.setDaemon(true);
        counterThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[Shutting down...]");
            running = false;

            // gracefully stop counter thread, should call before stop autoLock
            if (counterThread != null) {
                signalEvent();
                try {
                    counterThread.join();
                } catch (InterruptedException e) {
                    System.err.println("shutdown hook interrupted");
                    Thread.currentThread().interrupt();
                }
            }

            DtTime timeout = new DtTime(3, TimeUnit.SECONDS);
            // then stop autoLock, will trigger onLost event
            autoLock.stop(timeout);
            kvClient.stop(timeout);
            System.out.println("[Shutdown complete]");
        }));

        autoLock.start();
        System.out.println("[Auto-renewal autoLock started, waiting to acquire autoLock...]");
    }

    private static void signalEvent() {
        reentrantLock.lock();
        try {
            lockEventCondition.signalAll();
        } finally {
            reentrantLock.unlock();
        }
    }

    private static void runUpdater(KvClient kvClient, AutoRenewalLock autoLock) {
        while (running) {
            // the distributed lock may be expired at any time, the AutoRenewalLock makes the best effort
            // to renew it and notify when it expires, but it still a good idea to check it before doing other things
            if (autoLock.isHeldByCurrentClient()) {
                try {
                    KvNode node = kvClient.get(GROUP_ID, COUNTER_KEY.getBytes());
                    int currentValue = 0;
                    if (node != null && node.data != null) {
                        currentValue = Integer.parseInt(new String(node.data));
                    }

                    int newValue = currentValue + 1;
                    kvClient.put(GROUP_ID, COUNTER_KEY.getBytes(), String.valueOf(newValue).getBytes());
                    System.out.println("[Counter updated: " + newValue + "]");
                } catch (Exception e) {
                    System.err.println("[Error updating counter: " + e.getMessage() + "]");
                    break;
                }
                if (!running) {
                    break;
                }
                reentrantLock.lock();
                try {
                    if (!running) {
                        break;
                    }
                    //noinspection ResultOfMethodCallIgnored
                    lockEventCondition.await(UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    System.err.println("counter thread interrupted");
                    Thread.currentThread().interrupt();
                } finally {
                    reentrantLock.unlock();
                }
            } else {
                reentrantLock.lock();
                try {
                    lockEventCondition.await();
                } catch (InterruptedException e) {
                    System.err.println("counter thread interrupted");
                    Thread.currentThread().interrupt();
                } finally {
                    reentrantLock.unlock();
                }
            }
        }
        System.out.println("[Counter thread exit]");
    }
}

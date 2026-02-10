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
import com.github.dtprj.dongting.dtkv.DistributedLock;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvNode;

import java.util.concurrent.TimeUnit;

/**
 * Distributed lock demo with counter increment.
 * <p>
 * NOTE: Before running this demo, start the server first:
 *   ./bin/start-dongting.sh
 *
 * @author huangli
 */
public class DistributedLockDemo {

    private static final int GROUP_ID = 0;
    private static final String SERVERS = "1,127.0.0.1:9332";
    private static final int[] MEMBERS = {1};

    private static final String DIR_NAME = "lock_dir";
    private static final String COUNTER_KEY = "lock_dir.counter";
    private static final String LOCK_NAME = "lock_dir.lock";
    private static final long LEASE_MILLIS = 60000;
    private static final long WAIT_TIMEOUT_MILLIS = 10000;

    public static void main(String[] args) throws Exception {
        KvClient kvClient = new KvClient();
        kvClient.start();
        boolean lockAcquired = false;
        DistributedLock lock = null;

        try {
            kvClient.getRaftClient().clientAddNode(SERVERS);
            kvClient.getRaftClient().clientAddOrUpdateGroup(GROUP_ID, MEMBERS);

            kvClient.mkdir(GROUP_ID, DIR_NAME.getBytes());

            // it's recommended to create lock instance once and always reuse it to tryLock/unlock
            lock = kvClient.createLock(GROUP_ID, LOCK_NAME.getBytes());

            lockAcquired = lock.tryLock(LEASE_MILLIS, WAIT_TIMEOUT_MILLIS);
            if (!lockAcquired) {
                System.out.println("Failed to acquire lock");
                return;
            }

            KvNode node = kvClient.get(GROUP_ID, COUNTER_KEY.getBytes());
            int value = 0;
            if (node != null && node.data != null) {
                value = Integer.parseInt(new String(node.data));
            }
            System.out.println("Current value: " + value);

            Thread.sleep(5000);

            kvClient.put(GROUP_ID, COUNTER_KEY.getBytes(), String.valueOf(value + 1).getBytes());
            System.out.println("Incremented to: " + (value + 1));
        } finally {
            if (lockAcquired) {
                lock.unlock();
            }
            kvClient.stop(new DtTime(3, TimeUnit.SECONDS));
        }
    }
}

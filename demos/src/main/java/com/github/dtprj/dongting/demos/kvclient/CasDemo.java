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
import com.github.dtprj.dongting.dtkv.KvClient;

import java.util.concurrent.TimeUnit;

/**
 * Demo showing compareAndSet (CAS) usage.
 * <p>
 * NOTE: Before running this demo, start the server first:
 *   ./bin/start-dongting.sh
 *
 * @author huangli
 */
public class CasDemo {

    private static final int GROUP_ID = 0;
    private static final String SERVERS = "1,127.0.0.1:9332";
    private static final int[] MEMBERS = {1};

    public static void main(String[] args) throws Exception {
        KvClient kvClient = new KvClient();
        kvClient.start();

        try {
            kvClient.getRaftClient().clientAddNode(SERVERS);
            kvClient.getRaftClient().clientAddOrUpdateGroup(GROUP_ID, MEMBERS);

            System.out.println("Connected to " + SERVERS);
            System.out.println();

            kvClient.mkdir(GROUP_ID, "cas".getBytes());
            kvClient.put(GROUP_ID, "cas.counter".getBytes(), "0".getBytes());
            System.out.println("Initial value: 0");

            // 1. CAS success
            System.out.println("\n=== CAS success ===");
            byte[] current = kvClient.get(GROUP_ID, "cas.counter".getBytes()).data;
            boolean success = kvClient.compareAndSet(GROUP_ID, "cas.counter".getBytes(), current, "1".getBytes());
            System.out.println("CAS compareAndSet(1): " + (success ? "success" : "failed"));
            System.out.println("New value: " + new String(kvClient.get(GROUP_ID, "cas.counter".getBytes()).data));

            // 2. CAS failed
            System.out.println("\n=== CAS failed ===");
            success = kvClient.compareAndSet(GROUP_ID, "cas.counter".getBytes(), "0".getBytes(), "2".getBytes());
            System.out.println("CAS compareAndSet(2) with wrong expected value: " + (success ? "success" : "failed"));
            System.out.println("Value unchanged: " + new String(kvClient.get(GROUP_ID, "cas.counter".getBytes()).data));

            // 3. CAS create new key
            System.out.println("\n=== CAS create new key ===");
            success = kvClient.compareAndSet(GROUP_ID, "cas.newkey".getBytes(), null, "created".getBytes());
            System.out.println("CAS create newkey: " + (success ? "success" : "failed"));
            System.out.println("Value: " + new String(kvClient.get(GROUP_ID, "cas.newkey".getBytes()).data));

            // 4. CAS delete
            System.out.println("\n=== CAS delete ===");
            current = kvClient.get(GROUP_ID, "cas.counter".getBytes()).data;
            success = kvClient.compareAndSet(GROUP_ID, "cas.counter".getBytes(), current, null);
            System.out.println("CAS delete counter: " + (success ? "success" : "failed"));
            System.out.println("Exists: " + (kvClient.get(GROUP_ID, "cas.counter".getBytes()) == null ? "no" : "yes"));

            System.out.println("\nCAS demo completed!");

        } finally {
            // Cleanup
            try {
                kvClient.remove(GROUP_ID, "cas.newkey".getBytes());
                kvClient.remove(GROUP_ID, "cas".getBytes());
            } catch (Exception e) {
                // ignore
            }
            kvClient.stop(new DtTime(3, TimeUnit.SECONDS));
            System.out.println("Client stopped.");
        }
    }
}

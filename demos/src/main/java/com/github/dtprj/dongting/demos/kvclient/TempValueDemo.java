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
import com.github.dtprj.dongting.dtkv.KvNode;

import java.util.concurrent.TimeUnit;

/**
 * Demo showing putTemp with TTL expiration.
 * <p>
 * NOTE: Before running this demo, start the server first:
 *   ./bin/start-dongting.sh
 *
 * @author huangli
 */
public class TempValueDemo {

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

            kvClient.mkdir(GROUP_ID, "temp".getBytes());

            // 1. putTemp with 1 second TTL
            System.out.println("=== putTemp demo ===");
            kvClient.putTemp(GROUP_ID, "temp.key1".getBytes(), "tempValue".getBytes(), 1000);
            System.out.println("Put: temp.key1 with TTL=1s");

            KvNode node = kvClient.get(GROUP_ID, "temp.key1".getBytes());
            System.out.println("Get before expire: " + (node == null ? "null" : new String(node.data)));

            // 2. Wait for expiration
            System.out.println("\n=== Wait for expiration ===");
            System.out.println("Waiting 1500ms...");
            Thread.sleep(1500);

            node = kvClient.get(GROUP_ID, "temp.key1".getBytes());
            System.out.println("Get after expire: " + (node == null ? "null (expired)" : "found"));

            System.out.println("\nTempValue demo completed!");

        } finally {
            // Cleanup: only need to remove "temp" dir, temp.key1 already expired
            try {
                kvClient.remove(GROUP_ID, "temp".getBytes());
            } catch (Exception e) {
                // ignore
            }
            kvClient.stop(new DtTime(3, TimeUnit.SECONDS));
            System.out.println("Client stopped.");
        }
    }
}

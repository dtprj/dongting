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
 * Simple client demo showing basic KvClient usage.
 * <p>
 * NOTE: Before running this demo, start the server first:
 *   ./bin/start-dongting.sh
 *
 * @author huangli
 */
public class SimpleDemo {

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

            // 1. mkdir
            System.out.println("=== mkdir demo ===");
            String dirKey = "demo";
            kvClient.mkdir(GROUP_ID, dirKey.getBytes());
            System.out.println("Created: " + dirKey);

            // 2. put
            System.out.println("\n=== put demo ===");
            kvClient.put(GROUP_ID, "demo.key1".getBytes(), "Hello Dongting!".getBytes());
            System.out.println("Put: demo.key1 = Hello Dongting!");

            kvClient.put(GROUP_ID, "demo.key2".getBytes(), "Dongting is fast!".getBytes());
            System.out.println("Put: demo.key2 = Dongting is fast!");

            // 3. get
            System.out.println("\n=== get demo ===");
            KvNode node1 = kvClient.get(GROUP_ID, "demo.key1".getBytes());
            System.out.println("Get demo.key1: " + (node1 == null ? "null" : new String(node1.data)));

            KvNode node2 = kvClient.get(GROUP_ID, "demo.key2".getBytes());
            System.out.println("Get demo.key2: " + (node2 == null ? "null" : new String(node2.data)));

            KvNode notExist = kvClient.get(GROUP_ID, "demo.notexist".getBytes());
            System.out.println("Get demo.notexist: " + (notExist == null ? "null (expected)" : "found"));

            // 4. list
            System.out.println("\n=== list demo ===");
            kvClient.list(GROUP_ID, dirKey.getBytes()).forEach(result -> {
                KvNode node = result.getNode();
                String name = result.getKeyInDir().toString();
                System.out.println("  " + name + " = " + new String(node.data));
            });

            System.out.println("\nDemo completed!");

        } finally {
            // Cleanup
            try {
                kvClient.remove(GROUP_ID, "demo.key1".getBytes());
                kvClient.remove(GROUP_ID, "demo.key2".getBytes());
                kvClient.remove(GROUP_ID, "demo".getBytes());
            } catch (Exception e) {
                // ignore
            }
            kvClient.stop(new DtTime(3, TimeUnit.SECONDS));
            System.out.println("Client stopped.");
        }
    }
}

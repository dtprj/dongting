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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Demo showing batchGet/batchPut usage.
 * <p>
 *  NOTE: Before running this demo, start the server first:
 *   ./bin/start-dongting.sh
 *
 * @author huangli
 */
public class BatchDemo {

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

            String dirKey = "batch";
            kvClient.mkdir(GROUP_ID, dirKey.getBytes());

            // 1. batchPut
            System.out.println("=== batchPut demo ===");
            List<byte[]> keys = new ArrayList<>();
            List<byte[]> values = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                keys.add(("batch.key" + i).getBytes());
                values.add(("batchValue" + i).getBytes());
            }
            kvClient.batchPut(GROUP_ID, keys, values);
            System.out.println("Put 5 keys: batch.key1~5");

            // 2. batchGet
            System.out.println("\n=== batchGet demo ===");
            List<KvNode> nodes = kvClient.batchGet(GROUP_ID, keys);
            for (int i = 0; i < nodes.size(); i++) {
                System.out.println("Get batch.key" + (i + 1) + ": " + new String(nodes.get(i).data));
            }

            // 3. batchGet with missing keys
            System.out.println("\n=== batchGet with missing keys ===");
            List<byte[]> mixedKeys = Arrays.asList("batch.key1".getBytes(), "batch.key999".getBytes());
            List<KvNode> mixedNodes = kvClient.batchGet(GROUP_ID, mixedKeys);
            System.out.println("Get batch.key1: " + new String(mixedNodes.get(0).data));
            System.out.println("Get batch.key999: " + (mixedNodes.get(1) == null ? "null (expected)" : "found"));

            // 4. batchRemove
            System.out.println("\n=== batchRemove demo ===");
            kvClient.batchRemove(GROUP_ID, Arrays.asList("batch.key1".getBytes(), "batch.key3".getBytes()));
            System.out.println("Removed batch.key1, batch.key3");

            // Verify
            System.out.println("\n=== Verify ===");
            List<KvNode> remaining = kvClient.batchGet(GROUP_ID, keys);
            for (int i = 0; i < remaining.size(); i++) {
                System.out.println("Get batch.key" + (i + 1) + ": " + (remaining.get(i) == null ? "removed" : "exists"));
            }

            System.out.println("\nBatch demo completed!");

        } finally {
            // Cleanup: only need to remove directory
            try {
                kvClient.remove(GROUP_ID, "batch".getBytes());
            } catch (Exception e) {
                // ignore
            }
            kvClient.stop(new DtTime(3, TimeUnit.SECONDS));
            System.out.println("Client stopped.");
        }
    }
}

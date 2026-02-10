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
 * Demo showing makeTempDir with TTL expiration.
 * <p>
 * NOTE: Before running this demo, start the server first:
 *   ./bin/start-dongting.sh
 *
 * @author huangli
 */
public class TempDirDemo {

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

            // 1. makeTempDir with 1 second TTL
            System.out.println("=== makeTempDir demo ===");
            kvClient.makeTempDir(GROUP_ID, "tempdir".getBytes(), 1000);
            System.out.println("Put: tempdir with TTL=1s");

            kvClient.put(GROUP_ID, "tempdir.child1".getBytes(), "childValue".getBytes());
            System.out.println("Added child: tempdir.child1");

            // 2. Wait for expiration
            System.out.println("\n=== Wait for expiration ===");
            System.out.println("Waiting 1500ms...");
            Thread.sleep(1500);

            KvNode node = kvClient.get(GROUP_ID, "tempdir".getBytes());
            System.out.println("Get tempdir: " + (node == null ? "null (expired)" : "found"));

            node = kvClient.get(GROUP_ID, "tempdir.child1".getBytes());
            System.out.println("Get tempdir.child1: " + (node == null ? "null (expired with parent)" : "found"));

            System.out.println("\nTempDir demo completed!");

        } finally {
            kvClient.stop(new DtTime(3, TimeUnit.SECONDS));
            System.out.println("Client stopped.");
        }
    }
}

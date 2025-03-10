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
package com.github.dtprj.dongting.demos.base;

import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftNode;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author huangli
 */
public abstract class DemoClientBase {

    private static final byte[] DATA = new byte[256];

    private static final DtLog log = DtLogs.getLogger(DemoClientBase.class);

    public static KvClient putAndGetFixCount(int groupId, String servers, int loopCount) throws Exception {
        KvClient kvClient = new KvClient();
        kvClient.start();
        List<RaftNode> nodes = RaftNode.parseServers(servers);
        kvClient.getRaftClient().clientAddNode(servers);
        int[] nodeIds = nodes.stream().mapToInt(n -> n.nodeId).toArray();
        kvClient.getRaftClient().clientAddOrUpdateGroup(groupId, nodeIds);
        kvClient.getRaftClient().fetchLeader(groupId).get();

        long t1 = System.currentTimeMillis();
        CountDownLatch latch1 = new CountDownLatch(loopCount);
        for (int i = 0; i < loopCount; i++) {
            String key = "key" + (i % 10_000);
            kvClient.put(groupId, key.getBytes(), DATA, (result, ex) -> {
                if (ex == null) {
                    latch1.countDown();
                } else {
                    FutureCallback.log.error("", ex);
                    System.exit(1);
                }
            });
        }
        latch1.await();
        t1 = System.currentTimeMillis() - t1;

        long t2 = System.currentTimeMillis();
        CountDownLatch latch2 = new CountDownLatch(loopCount);
        for (int i = 0; i < loopCount; i++) {
            String key = "key" + (i % 10_000);
            kvClient.get(groupId, key.getBytes(), (result, ex) -> {
                if (ex == null) {
                    latch2.countDown();
                } else {
                    log.error("", ex);
                    System.exit(1);
                }
            });
        }
        latch2.await();
        t2 = System.currentTimeMillis() - t2;


        System.out.println("----------------------------------------------");
        System.out.println("Unbelievable! " + loopCount + " linearizable puts finished in " + t1 + " ms, "
                + loopCount + " linearizable lease gets finished in " + t2 + " ms");
        System.out.println("Throughput: " + loopCount * 1000L / t1 + " puts/s, " + loopCount * 1000L / t2 + " gets/s");
        System.out.println(System.getProperty("os.name") + " with " + Runtime.getRuntime().availableProcessors() + " cores");
        System.out.println("----------------------------------------------");
        return kvClient;
    }
}

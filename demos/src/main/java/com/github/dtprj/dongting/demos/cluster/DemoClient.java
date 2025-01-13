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
package com.github.dtprj.dongting.demos.cluster;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class DemoClient {
    private static final DtLog log = DtLogs.getLogger(DemoClient.class);

    public static void main(String[] args) throws Exception {
        String servers = "1,127.0.0.1:5001;2,127.0.0.1:5002;3,127.0.0.1:5003";
        int groupId = 0;
        final int loop = 10_000;

        KvClient kvClient = new KvClient();
        kvClient.start();
        kvClient.getRaftClient().addOrUpdateGroup(groupId, servers);
        kvClient.getRaftClient().fetchLeader(groupId).get();

        long t1 = System.currentTimeMillis();
        CountDownLatch latch1 = new CountDownLatch(loop);
        for (int i = 0; i < loop; i++) {
            String key = "key" + (i % 10_000);
            DtTime timeout = new DtTime(3, TimeUnit.SECONDS);
            CompletableFuture<Void> f = kvClient.put(groupId, key, "value".getBytes(), timeout);
            f.whenComplete((v, e) -> {
                if (e == null) {
                    latch1.countDown();
                } else {
                    log.error("", e);
                    System.exit(1);
                }
            });
        }
        latch1.await();
        t1 = System.currentTimeMillis() - t1;

        long t2 = System.currentTimeMillis();
        CountDownLatch latch2 = new CountDownLatch(loop);
        for (int i = 0; i < loop; i++) {
            String key = "key" + (i % 10_000);
            DtTime timeout = new DtTime(3, TimeUnit.SECONDS);
            CompletableFuture<KvNode> f = kvClient.get(groupId, key, timeout);
            f.whenComplete((v, e) -> {
                if (e == null) {
                    latch2.countDown();
                } else {
                    log.error("", e);
                    System.exit(1);
                }
            });
        }
        latch2.await();
        t2 = System.currentTimeMillis() - t2;


        System.out.println("----------------------------------------------");
        System.out.println("Unbelievable! " + loop + " puts finished in " + t1 + " ms, gets finished in " + t2 + " ms");
        System.out.println("Throughput: " + loop * 1000 / t1 + " puts/s, " + loop * 1000 / t2 + " gets/s");
        System.out.println(System.getProperty("os.name") + " with " + Runtime.getRuntime().availableProcessors() + " cores");
        System.out.println("----------------------------------------------");

        kvClient.stop(new DtTime(3, TimeUnit.SECONDS));
    }
}

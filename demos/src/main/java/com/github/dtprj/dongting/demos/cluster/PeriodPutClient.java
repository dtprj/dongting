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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class PeriodPutClient implements GroupId {

    private static final DtLog log = DtLogs.getLogger(PeriodPutClient.class);

    public static void main(String[] args) throws Exception {
        String servers = "1,127.0.0.1:5001;2,127.0.0.1:5002;3,127.0.0.1:5003";
        KvClient kvClient = new KvClient();
        kvClient.start();
        kvClient.getRaftClient().clientAddNode(servers);
        kvClient.getRaftClient().clientAddOrUpdateGroup(GROUP_ID, new int[]{1, 2, 3});
        kvClient.getRaftClient().fetchLeader(GROUP_ID).get();

        long count = 1;
        while (true) {
            try {
                String key = "key" + ((count++) % 10_000);
                long t = System.currentTimeMillis();
                kvClient.put(GROUP_ID, key, "value".getBytes(), new DtTime(10, TimeUnit.SECONDS));
                log.info("put key " + key + " cost " + (System.currentTimeMillis() - t) + "ms");
            } catch (Exception e) {
                log.error("put key fail: {}", e.toString());
            }
            Thread.sleep(1000);
        }
    }
}

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
package com.github.dtprj.dongting.demos.lock;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.demos.base.DemoClientBase;
import com.github.dtprj.dongting.dtkv.DistributedLock;
import com.github.dtprj.dongting.dtkv.KvClient;

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class LockDemoClient extends DemoClientBase implements GroupId {

    public static void main(String[] args) throws Exception {
        String servers = "1,127.0.0.1:5001"; // serverId,ip:servicePort
        KvClient kvClient = new KvClient();
        kvClient.start();
        kvClient.getRaftClient().clientAddNode(servers);
        kvClient.getRaftClient().clientAddOrUpdateGroup(GROUP_ID, new int[]{1});

        DistributedLock lock = kvClient.createLock(GROUP_ID, "lock1".getBytes());
        long t = System.currentTimeMillis();
        boolean lockSuccess = lock.tryLock(30000, 5000);
        System.out.println("--------------------------------------------");
        System.out.println("tryLock " + (lockSuccess ? "success" : "fail")
                + " in " + (System.currentTimeMillis() - t) + "ms");
        System.out.println("--------------------------------------------");
        if (lockSuccess) {
            lock.unlock();
        }
        kvClient.stop(new DtTime(3, TimeUnit.SECONDS));
    }
}

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
package com.github.dtprj.dongting.demos.standalone;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.demos.base.DemoClientBase;
import com.github.dtprj.dongting.dtkv.KvClient;

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class StandaloneDemoClient extends DemoClientBase implements GroupId {

    final static int LOOP_COUNT = 1_000_000;

    public static void main(String[] args) throws Exception {
        String servers = "1,127.0.0.1:5001"; // serverId,ip:servicePort
        KvClient client = putAndGetFixCount(GROUP_ID, servers, LOOP_COUNT);

        // System.exit(0);
        client.stop(new DtTime(3, TimeUnit.SECONDS));
    }
}

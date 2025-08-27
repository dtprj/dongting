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
package com.github.dtprj.dongting.demos.watch;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.demos.base.DemoClientBase;
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.dtkv.KvNode;
import com.github.dtprj.dongting.dtkv.WatchEvent;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class WatchDemoClient extends DemoClientBase implements GroupId {

    private static KvClient kvClient;

    public static void main(String[] args) throws Exception {
        String servers = "1,127.0.0.1:5001"; // serverId,ip:servicePort

        kvClient = new KvClient();
        kvClient.getWatchManager().setListener(WatchDemoClient::onUpdate, Executors.newSingleThreadExecutor());
        kvClient.start();
        kvClient.getRaftClient().clientAddNode(servers);
        kvClient.getRaftClient().clientAddOrUpdateGroup(GROUP_ID, new int[]{1});

        // init data key1, and dir1(without sub keys)
        kvClient.put(GROUP_ID, "key1".getBytes(), "key1_v1".getBytes());
        kvClient.mkdir(GROUP_ID, "dir1".getBytes());

        kvClient.getWatchManager().addWatch(GROUP_ID, "key1".getBytes(), "dir1".getBytes());

        kvClient.put(GROUP_ID, "key1".getBytes(), "key1_v2".getBytes());
        kvClient.put(GROUP_ID, "dir1.key2".getBytes(), "key2_v1".getBytes());
        kvClient.put(GROUP_ID, "dir1.key3".getBytes(), "key3_v1".getBytes());

        // if there is no sleep, events for dir1 may be squashed into single STATE_NOT_EXISTS event
        Thread.sleep(500);

        kvClient.remove(GROUP_ID, "dir1.key2".getBytes());
        kvClient.remove(GROUP_ID, "dir1.key3".getBytes());
        kvClient.remove(GROUP_ID, "dir1".getBytes());

        // wait for events before exit
        Thread.sleep(3000);

        // System.exit(0);
        kvClient.stop(new DtTime(3, TimeUnit.SECONDS));
    }

    private static void onUpdate(WatchEvent event) {
        System.out.println("--------------------------");
        String key = new String(event.key);
        switch (event.state) {
            case WatchEvent.STATE_NOT_EXISTS:
                // in this case event.value is null
                System.out.println("get STATE_NOT_EXISTS event: key=" + key+ ", raftLogIndex=" + event.raftIndex);
                break;
            case WatchEvent.STATE_VALUE_EXISTS:
                System.out.println("get STATE_VALUE_EXISTS event: key=" + key + ", value=" + new String(event.value) +
                        ", raftLogIndex=" + event.raftIndex);
                break;
            case WatchEvent.STATE_DIRECTORY_EXISTS:
                // in this case event.value is null
                System.out.println("get STATE_DIRECTORY_EXISTS event: dir=" + key + ", raftLogIndex=" + event.raftIndex);
                // we should check the sub keys by ourselves
                KvNode n2 = kvClient.get(GROUP_ID, "dir1.key2".getBytes());
                KvNode n3 = kvClient.get(GROUP_ID, "dir1.key3".getBytes());
                System.out.println("dir1.key2=" + new String(n2.data) + ", dir1.key3=" + new String(n3.data));
                break;
        }
    }
}

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
import com.github.dtprj.dongting.dtkv.WatchEvent;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demo showing watch functionality.
 * <p>
 * NOTE: Before running this demo, start the server first:
 *   ./bin/start-dongting.sh
 *<p>
 * NOTE: Watch notifications provide eventual consistency. Multiple changes may be coalesced
 * into a single notification. The demo expects only one event for v1 and v2 changes.
 *<p>
 * After call addWatch, the server may push watch notification immediately for current value of
 * the keys.
 *
 * @author huangli
 */
public class WatchDemo {

    private static final int GROUP_ID = 0;
    private static final String SERVERS = "1,127.0.0.1:9332";
    private static final int[] MEMBERS = {1};

    private static final AtomicInteger eventCount = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        KvClient kvClient = new KvClient();
        kvClient.getWatchManager().setListener(WatchDemo::onWatchEvent,
                Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "WatchDemo");
                    t.setDaemon(true);
                    return t;
                }));

        kvClient.start();

        try {
            kvClient.getRaftClient().clientAddNode(SERVERS);
            kvClient.getRaftClient().clientAddOrUpdateGroup(GROUP_ID, MEMBERS);

            System.out.println("Connected to " + SERVERS);
            System.out.println();

            kvClient.mkdir(GROUP_ID, "watch".getBytes());
            kvClient.put(GROUP_ID, "watch.key1".getBytes(), "initial".getBytes());

            // 1. Watch a key
            System.out.println("=== Watch key ===");
            kvClient.getWatchManager().addWatch(GROUP_ID, "watch.key1".getBytes());
            System.out.println("Watching: watch.key1");

            kvClient.put(GROUP_ID, "watch.key1".getBytes(), "v1".getBytes());
            kvClient.put(GROUP_ID, "watch.key1".getBytes(), "v2".getBytes());
            Thread.sleep(1000);
            System.out.println("Events received: " + eventCount.get());

            // 2. Watch a directory
            System.out.println("\n=== Watch directory ===");
            kvClient.getWatchManager().addWatch(GROUP_ID, "watch".getBytes());
            System.out.println("Watching: watch");

            kvClient.put(GROUP_ID, "watch.key2".getBytes(), "key2_value".getBytes());
            kvClient.put(GROUP_ID, "watch.key3".getBytes(), "key3_value".getBytes());
            kvClient.remove(GROUP_ID, "watch.key2".getBytes());
            Thread.sleep(1000);
            System.out.println("Total events: " + eventCount.get());

            // 3. Remove watch
            System.out.println("\n=== Remove watch ===");
            kvClient.getWatchManager().removeWatch(GROUP_ID, "watch.key1".getBytes());
            kvClient.getWatchManager().removeWatch(GROUP_ID, "watch".getBytes());
            System.out.println("Watches removed");

            int before = eventCount.get();
            kvClient.put(GROUP_ID, "watch.key1".getBytes(), "after_unwatch".getBytes());
            Thread.sleep(1000);
            System.out.println("New events after unwatch: " + (eventCount.get() - before) + " (expected 0)");

            System.out.println("\nWatch demo completed!");
            System.out.println("Total events: " + eventCount.get());

        } finally {
            // Cleanup
            kvClient.getWatchManager().removeWatch(GROUP_ID, "watch.key1".getBytes());
            kvClient.getWatchManager().removeWatch(GROUP_ID, "watch".getBytes());
            try {
                kvClient.remove(GROUP_ID, "watch.key1".getBytes());
                kvClient.remove(GROUP_ID, "watch.key3".getBytes());
                kvClient.remove(GROUP_ID, "watch".getBytes());
            } catch (Exception e) {
                // ignore
            }
            kvClient.stop(new DtTime(3, TimeUnit.SECONDS));
            System.out.println("Client stopped.");
        }
    }

    private static void onWatchEvent(WatchEvent event) {
        eventCount.incrementAndGet();
        String key = new String(event.key);
        switch (event.state) {
            case WatchEvent.STATE_NOT_EXISTS:
                System.out.println("  [Event] KEY_DELETED: " + key);
                break;
            case WatchEvent.STATE_VALUE_EXISTS:
                System.out.println("  [Event] VALUE_CHANGED: " + key + " = " + new String(event.value));
                break;
            case WatchEvent.STATE_DIRECTORY_EXISTS:
                System.out.println("  [Event] DIR_CHANGED: " + key);
                break;
        }
    }
}

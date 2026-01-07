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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.WatchEvent;
import com.github.dtprj.dongting.dtkv.WatchNotifyReq;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.DtChannel;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.NioNet;
import com.github.dtprj.dongting.net.Peer;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.raft.test.TestUtil;
import com.github.dtprj.dongting.util.MockRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class ServerWatchManagerTest {

    private final UUID selfUuid = UUID.randomUUID();

    private KvServerConfig kvConfig;
    private ServerWatchManager manager;
    private KvImpl kv;
    private Timestamp ts;
    private ReadPacket<WatchNotifyRespCallback> rpcResult;
    private Throwable rpcEx;
    private long raftIndex;

    private static MockDtChannel dtc1;
    private static MockDtChannel dtc2;
    private static MockDtChannel dtc3;

    private LinkedList<PushReqInfo> pushRequestList;
    private LinkedList<Runnable> tasks;

    @BeforeAll
    public static void initStatic() throws Exception {
        dtc1 = new MockDtChannel();
        dtc1.channel = SocketChannel.open();
        dtc2 = new MockDtChannel();
        dtc2.channel = SocketChannel.open();
        dtc3 = new MockDtChannel();
        dtc3.channel = SocketChannel.open();
    }

    @AfterAll
    public static void closeStatic() throws Exception {
        dtc1.channel.close();
        dtc2.channel.close();
        dtc3.channel.close();
    }

    private static class PushReqInfo {
        final ChannelInfo ci;
        final WatchNotifyReq req;
        final ArrayList<ChannelWatch> watchList;
        final int requestEpoch;
        final boolean fireNext;

        PushReqInfo(ChannelInfo ci, WatchNotifyReq req, ArrayList<ChannelWatch> watchList,
                    int requestEpoch, boolean fireNext) {
            this.ci = ci;
            this.req = req;
            this.watchList = watchList;
            this.requestEpoch = requestEpoch;
            this.fireNext = fireNext;
        }
    }

    @BeforeEach
    public void setup() {
        ts = new Timestamp();
        kvConfig = new KvServerConfig();
        pushRequestList = new LinkedList<>();
        tasks = new LinkedList<>();
        raftIndex = 1;
        int groupId = 0;
        LinkedList<PushReqInfo> q = pushRequestList;
        this.rpcEx = null;
        this.rpcResult = null;
        manager = new ServerWatchManager(groupId, ts, kvConfig, new long[]{1, 1000}) {
            @Override
            protected void sendRequest(ChannelInfo ci, WatchNotifyReq req, ArrayList<ChannelWatch> watchList,
                                       int requestEpoch, boolean fireNext) {
                q.add(new PushReqInfo(ci, req, watchList, requestEpoch, fireNext));
                Runnable run = () -> {
                    try {
                        ReadPacket<WatchNotifyRespCallback> r = rpcResult;
                        if (rpcEx == null && r == null) {
                            int[] codes = new int[watchList.size()];
                            for (int i = 0; i < codes.length; i++) {
                                codes[i] = CmdCodes.SUCCESS;
                            }
                            r = createRpcResult(codes);
                        }
                        manager.processNotifyResult(ci, watchList, r, rpcEx, requestEpoch, fireNext);
                    } catch (Exception e) {
                        fail();
                    }
                };
                tasks.add(run);
            }
        };
        TtlManager tm = new TtlManager(ts, null);
        kv = new KvImpl(manager, tm, ts, groupId, 16, 0.75f);
        put("aaa", "bbb"); // add first item and make raft index in statemachine greater than 0
    }

    private void mockClientResponse() {
        // prevent the callback may update tasks while iterating
        ArrayList<Runnable> tasksCopy = new ArrayList<>(tasks);
        tasks.clear();
        Collections.shuffle(tasksCopy);
        for (Runnable r : tasksCopy) {
            r.run();
        }
    }

    @Test
    public void testAddOrUpdateActiveQueue() {
        ChannelInfo ci1 = new ChannelInfo(dtc1, ts.nanoTime);
        ChannelInfo ci2 = new ChannelInfo(dtc2, ts.nanoTime);
        ChannelInfo ci3 = new ChannelInfo(dtc3, ts.nanoTime);

        manager.addOrUpdateActiveQueue(ci1);
        assertEquals(ci1, manager.activeQueueHead);
        assertEquals(ci1, manager.activeQueueTail);

        manager.addOrUpdateActiveQueue(ci1);
        assertEquals(ci1, manager.activeQueueHead);
        assertEquals(ci1, manager.activeQueueTail);

        manager.addOrUpdateActiveQueue(ci2);
        assertEquals(ci1, manager.activeQueueHead);
        assertEquals(ci2, manager.activeQueueTail);
        assertSame(ci1.next, ci2);
        assertSame(ci2.prev, ci1);

        manager.addOrUpdateActiveQueue(ci3);
        assertEquals(ci1, manager.activeQueueHead);
        assertEquals(ci3, manager.activeQueueTail);

        manager.addOrUpdateActiveQueue(ci1);
        assertEquals(ci2, manager.activeQueueHead);
        assertEquals(ci1, manager.activeQueueTail);

        manager.addOrUpdateActiveQueue(ci3);
        assertEquals(ci2, manager.activeQueueHead);
        assertEquals(ci3, manager.activeQueueTail);
    }

    @Test
    public void testRemoveFromActiveQueue() {
        ChannelInfo ci1 = new ChannelInfo(dtc1, ts.nanoTime);
        ChannelInfo ci2 = new ChannelInfo(dtc2, ts.nanoTime);
        ChannelInfo ci3 = new ChannelInfo(dtc3, ts.nanoTime);

        manager.addOrUpdateActiveQueue(ci1);
        manager.removeFromActiveQueue(ci1);
        assertNull(manager.activeQueueHead);
        assertNull(manager.activeQueueTail);

        manager.addOrUpdateActiveQueue(ci1);
        manager.addOrUpdateActiveQueue(ci2);
        manager.removeFromActiveQueue(ci1);
        assertEquals(ci2, manager.activeQueueHead);
        assertEquals(ci2, manager.activeQueueTail);

        manager.addOrUpdateActiveQueue(ci1);
        manager.addOrUpdateActiveQueue(ci2);
        manager.removeFromActiveQueue(ci2);
        assertEquals(ci1, manager.activeQueueHead);
        assertEquals(ci1, manager.activeQueueTail);

        manager.addOrUpdateActiveQueue(ci1);
        manager.addOrUpdateActiveQueue(ci2);
        manager.addOrUpdateActiveQueue(ci3);
        manager.removeFromActiveQueue(ci1);
        assertEquals(ci2, manager.activeQueueHead);
        assertEquals(ci3, manager.activeQueueTail);

        manager.addOrUpdateActiveQueue(ci1);
        manager.addOrUpdateActiveQueue(ci2);
        manager.addOrUpdateActiveQueue(ci3);
        manager.removeFromActiveQueue(ci2);
        assertEquals(ci1, manager.activeQueueHead);
        assertEquals(ci3, manager.activeQueueTail);

        manager.addOrUpdateActiveQueue(ci1);
        manager.addOrUpdateActiveQueue(ci2);
        manager.addOrUpdateActiveQueue(ci3);
        manager.removeFromActiveQueue(ci3);
        assertEquals(ci1, manager.activeQueueHead);
        assertEquals(ci2, manager.activeQueueTail);
    }

    private long getNotifiedIndex(DtChannel dtc, String key) {
        ChannelInfo ci = manager.channelInfoMap.get(dtc);
        ChannelWatch cw = ci.watches.get(new ByteArray(key.getBytes(StandardCharsets.UTF_8)));
        return cw.notifiedIndex;
    }

    // basic test
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSync_basic(boolean takeSnapshot) {
        if (takeSnapshot) {
            KvImplTest.takeSnapshot(kv);
        }
        long expectIndex = raftIndex - 1;

        manager.sync(kv, dtc1, false, keys("key1"), new long[]{0});
        assertTrue(manager.dispatch());
        mockClientResponse();
        assertTrue(manager.dispatch());
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        PushReqInfo pushReqInfo = pushRequestList.poll();
        assertEquals(dtc1, pushReqInfo.ci.channel);
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertNull(pushReqInfo.req.notifyList.get(0).value);
        assertEquals(WatchEvent.STATE_NOT_EXISTS, pushReqInfo.req.notifyList.get(0).state);
        assertEquals(expectIndex, pushReqInfo.req.notifyList.get(0).raftIndex);

        expectIndex = raftIndex;
        put("key1", "value1");
        put("key2", "value2");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(dtc1, pushReqInfo.ci.channel);
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertEquals("value1", new String(pushReqInfo.req.notifyList.get(0).value));
        assertEquals(WatchEvent.STATE_VALUE_EXISTS, pushReqInfo.req.notifyList.get(0).state);
        assertEquals(expectIndex, pushReqInfo.req.notifyList.get(0).raftIndex);

        manager.sync(kv, dtc1, false, keys("key1", "key2"),
                new long[]{getNotifiedIndex(dtc1, "key1"), 0});
        expectIndex = raftIndex;
        put("key1", "value1_2");
        put("key2", "value2_2");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(dtc1, pushReqInfo.ci.channel);
        assertEquals(2, pushReqInfo.req.notifyList.size());
        assertEquals("key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertEquals("value1_2", new String(pushReqInfo.req.notifyList.get(0).value));
        assertEquals(expectIndex, pushReqInfo.req.notifyList.get(0).raftIndex);
        assertEquals("key2", new String(pushReqInfo.req.notifyList.get(1).key));
        assertEquals("value2_2", new String(pushReqInfo.req.notifyList.get(1).value));
        assertEquals(expectIndex + 1, pushReqInfo.req.notifyList.get(1).raftIndex);

        manager.sync(kv, dtc2, true, keys("key1", "key2"), new long[]{0, 0});
        put("key1", "value1_3");
        put("key2", "value2_3");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(2, pushRequestList.size());
        for (int i = 0; i < 2; i++) {
            pushReqInfo = pushRequestList.poll();
            if (dtc1 == pushReqInfo.ci.channel) {
                assertEquals(2, pushReqInfo.req.notifyList.size());
                assertEquals("key1", new String(pushReqInfo.req.notifyList.get(0).key));
                assertEquals("value1_3", new String(pushReqInfo.req.notifyList.get(0).value));
                assertEquals("key2", new String(pushReqInfo.req.notifyList.get(1).key));
                assertEquals("value2_3", new String(pushReqInfo.req.notifyList.get(1).value));
            } else {
                assertEquals(dtc2, pushReqInfo.ci.channel);
                assertEquals(2, pushReqInfo.req.notifyList.size());
                assertEquals("key1", new String(pushReqInfo.req.notifyList.get(0).key));
                assertEquals("value1_3", new String(pushReqInfo.req.notifyList.get(0).value));
                assertEquals("key2", new String(pushReqInfo.req.notifyList.get(1).key));
                assertEquals("value2_3", new String(pushReqInfo.req.notifyList.get(1).value));
            }
        }

        manager.sync(kv, dtc1, true, keys("key2", "key3"), new long[]{getNotifiedIndex(dtc1, "key2"), 0});
        manager.sync(kv, dtc2, false, keys("key2"), new long[]{-1});
        put("key1", "value1_4");
        put("key2", "value2_4");
        put("key3", "value3_4");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(2, pushRequestList.size());
        for (int i = 0; i < 2; i++) {
            pushReqInfo = pushRequestList.poll();
            if (dtc1 == pushReqInfo.ci.channel) {
                assertEquals(2, pushReqInfo.req.notifyList.size());
                assertEquals("key2", new String(pushReqInfo.req.notifyList.get(0).key));
                assertEquals("value2_4", new String(pushReqInfo.req.notifyList.get(0).value));
                assertEquals("key3", new String(pushReqInfo.req.notifyList.get(1).key));
                assertEquals("value3_4", new String(pushReqInfo.req.notifyList.get(1).value));
            } else {
                assertEquals(dtc2, pushReqInfo.ci.channel);
                assertEquals(1, pushReqInfo.req.notifyList.size());
                assertEquals("key1", new String(pushReqInfo.req.notifyList.get(0).key));
                assertEquals("value1_4", new String(pushReqInfo.req.notifyList.get(0).value));
            }
        }

        manager.sync(kv, dtc1, true, keys(), new long[]{});
        manager.sync(kv, dtc2, true, keys(), new long[]{});
        put("key1", "value1_5");
        put("key2", "value2_5");
        put("key3", "value3_5");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(0, pushRequestList.size());
    }

    // tree structure watch test
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSync_tree(boolean takeSnapshot) {
        if (takeSnapshot) {
            KvImplTest.takeSnapshot(kv);
        }
        manager.sync(kv, dtc1, false, keys("dir1.dir2.key1"), new long[]{0});
        mkdir("dir1");
        mkdir("dir1.dir2");
        put("dir1.dir2.key1", "value1");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        PushReqInfo pushReqInfo = pushRequestList.poll();
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("dir1.dir2.key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertEquals("value1", new String(pushReqInfo.req.notifyList.get(0).value));
        assertEquals(WatchEvent.STATE_VALUE_EXISTS, pushReqInfo.req.notifyList.get(0).state);

        // add dir1 watch
        manager.sync(kv, dtc1, false, keys("dir1"), new long[]{0});
        long expectIndex = raftIndex;
        put("dir1.dir2.key1", "value1_2");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(2, pushReqInfo.req.notifyList.size());
        // the sync operation for dir1 make it is the first one
        assertEquals("dir1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertNull(pushReqInfo.req.notifyList.get(0).value);
        assertEquals(WatchEvent.STATE_DIRECTORY_EXISTS, pushReqInfo.req.notifyList.get(0).state);
        assertEquals(expectIndex, pushReqInfo.req.notifyList.get(0).raftIndex);
        assertEquals("dir1.dir2.key1", new String(pushReqInfo.req.notifyList.get(1).key));
        assertEquals("value1_2", new String(pushReqInfo.req.notifyList.get(1).value));
        assertEquals(WatchEvent.STATE_VALUE_EXISTS, pushReqInfo.req.notifyList.get(1).state);
        assertEquals(expectIndex, pushReqInfo.req.notifyList.get(1).raftIndex);

        expectIndex = raftIndex;
        remove("dir1.dir2.key1");
        remove("dir1.dir2");
        remove("dir1");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(2, pushReqInfo.req.notifyList.size());
        assertEquals("dir1.dir2.key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertNull(pushReqInfo.req.notifyList.get(0).value);
        assertEquals(WatchEvent.STATE_NOT_EXISTS, pushReqInfo.req.notifyList.get(0).state);
        assertEquals(expectIndex, pushReqInfo.req.notifyList.get(0).raftIndex);
        assertEquals("dir1", new String(pushReqInfo.req.notifyList.get(1).key));
        assertNull(pushReqInfo.req.notifyList.get(1).value);
        assertEquals(WatchEvent.STATE_NOT_EXISTS, pushReqInfo.req.notifyList.get(1).state);
        assertEquals(expectIndex + 2, pushReqInfo.req.notifyList.get(1).raftIndex);

        mkdir("dir1");
        mkdir("dir1.dir2");
        expectIndex = raftIndex;
        put("dir1.dir2.key1", "value1_3");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(2, pushReqInfo.req.notifyList.size());
        assertEquals("dir1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertNull(pushReqInfo.req.notifyList.get(0).value);
        assertEquals(WatchEvent.STATE_DIRECTORY_EXISTS, pushReqInfo.req.notifyList.get(0).state);
        assertEquals(expectIndex, pushReqInfo.req.notifyList.get(0).raftIndex);
        assertEquals("dir1.dir2.key1", new String(pushReqInfo.req.notifyList.get(1).key));
        assertEquals("value1_3", new String(pushReqInfo.req.notifyList.get(1).value));
        assertEquals(WatchEvent.STATE_VALUE_EXISTS, pushReqInfo.req.notifyList.get(1).state);
        assertEquals(expectIndex, pushReqInfo.req.notifyList.get(1).raftIndex);
    }

    // remove from kv tree when the watch holder is mount to parent watch holder
    @Test
    public void testSync_removeFromKvTreeWhenWatchHolderIsMountToParent() {
        manager.sync(kv, dtc1, false, keys("dir1.dir2.key1"), new long[]{0});
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        PushReqInfo pushReqInfo = pushRequestList.poll();
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("dir1.dir2.key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertNull(pushReqInfo.req.notifyList.get(0).value);
        assertEquals(WatchEvent.STATE_NOT_EXISTS, pushReqInfo.req.notifyList.get(0).state);

        manager.sync(kv, dtc1, false, keys("dir1.dir2.key1"), new long[]{-1});
        remove("dir1.dir2.key1");
        manager.dispatch();
        mockClientResponse();
        assertEquals(0, pushRequestList.size());
    }

    // mount to parent and the parent has no watch holder
    @Test
    public void testSync_mountToPrentWhenParentHasNoWatchHolder() {
        KvImplTest.takeSnapshot(kv);
        mkdir("dir1");
        mkdir("dir1.dir2");
        put("dir1.dir2.key1", "value1");
        manager.sync(kv, dtc1, false, keys("dir1.dir2.key1"), new long[]{0});
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        PushReqInfo pushReqInfo = pushRequestList.poll();
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("dir1.dir2.key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertEquals("value1", new String(pushReqInfo.req.notifyList.get(0).value));
        assertEquals(WatchEvent.STATE_VALUE_EXISTS, pushReqInfo.req.notifyList.get(0).state);

        // mount to parent and the parent has no watch holder
        remove("dir1.dir2.key1");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("dir1.dir2.key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertNull(pushReqInfo.req.notifyList.get(0).value);
        assertEquals(WatchEvent.STATE_NOT_EXISTS, pushReqInfo.req.notifyList.get(0).state);

        // change to directory
        mkdir("dir1.dir2.key1");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("dir1.dir2.key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertNull(pushReqInfo.req.notifyList.get(0).value);
        assertEquals(WatchEvent.STATE_DIRECTORY_EXISTS, pushReqInfo.req.notifyList.get(0).state);
    }

    // mount to removed node
    @Test
    public void testSync_mountToRemovedNode() {
        mkdir("dir1");
        mkdir("dir1.dir2");
        put("dir1.dir2.key1", "value1");
        KvImplTest.takeSnapshot(kv);
        remove("dir1.dir2.key1");
        manager.sync(kv, dtc1, false, keys("dir1.dir2.key1"), new long[]{0});
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        PushReqInfo pushReqInfo = pushRequestList.poll();
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("dir1.dir2.key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertNull(pushReqInfo.req.notifyList.get(0).value);
        assertEquals(WatchEvent.STATE_NOT_EXISTS, pushReqInfo.req.notifyList.get(0).state);

        put("dir1.dir2.key1", "value1_2");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("dir1.dir2.key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertEquals("value1_2", new String(pushReqInfo.req.notifyList.get(0).value));
        assertEquals(WatchEvent.STATE_VALUE_EXISTS, pushReqInfo.req.notifyList.get(0).state);
    }

    // change epoch (reset)
    @Test
    public void testSync_changeEpoch() {
        manager.sync(kv, dtc1, false, keys("key1"), new long[]{0});
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();

        long raftIndex = this.raftIndex;
        ChannelWatch cw = manager.activeQueueHead.watches.values().iterator().next();
        put("key1", "value1");
        manager.dispatch();
        manager.reset();
        mockClientResponse();
        assertTrue(cw.notifiedIndex < cw.notifiedIndexPending);
        assertEquals(raftIndex, cw.notifiedIndexPending);
    }

    // update during notify
    @Test
    public void testSync_updateDuringNotify() {
        manager.sync(kv, dtc1, false, keys("key1"), new long[]{0});
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        pushRequestList.clear();

        put("key1", "value1");
        manager.dispatch();
        put("key1", "value1_2");
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();

        assertEquals(2, pushRequestList.size());
        PushReqInfo pushReqInfo = pushRequestList.poll();
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertEquals("value1", new String(pushReqInfo.req.notifyList.get(0).value));
        assertEquals(WatchEvent.STATE_VALUE_EXISTS, pushReqInfo.req.notifyList.get(0).state);
        pushReqInfo = pushRequestList.poll();
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertEquals("value1_2", new String(pushReqInfo.req.notifyList.get(0).value));
        assertEquals(WatchEvent.STATE_VALUE_EXISTS, pushReqInfo.req.notifyList.get(0).state);
    }

    @Test
    public void testSync_removeOneWatchByResp() {
        manager.sync(kv, dtc1, false, keys("key1", "key2"), new long[]{0, 0});
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        pushRequestList.clear();

        setRpcResult(KvCodes.SUCCESS, KvCodes.REMOVE_WATCH);
        put("key1", "value1");
        put("key2", "value2");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        PushReqInfo pushReqInfo = pushRequestList.poll();
        assertEquals(2, pushReqInfo.req.notifyList.size());
        assertEquals("key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertEquals("key2", new String(pushReqInfo.req.notifyList.get(1).key));

        setRpcResult(KvCodes.REMOVE_WATCH);
        put("key1", "value1_2");
        put("key2", "value2_2");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(1, pushReqInfo.req.notifyList.size());
        assertEquals("key1", new String(pushReqInfo.req.notifyList.get(0).key));

        assertNull(manager.activeQueueHead);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSync_removeAllWatchByResp(boolean error) {
        manager.sync(kv, dtc1, false, keys("key1", "key2"), new long[]{0, 0});
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        pushRequestList.clear();

        if (error) {
            setRpcEx(new NetCodeException(CmdCodes.COMMAND_NOT_SUPPORT, "", null));
        } else {
            ReadPacket<WatchNotifyRespCallback> r = new ReadPacket<>();
            r.bizCode = KvCodes.REMOVE_ALL_WATCH;
            setRpcResult(r);
        }

        put("key1", "value1");
        put("key2", "value2");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();

        assertNull(manager.activeQueueHead);

        assertEquals(1, pushRequestList.size());
        PushReqInfo pushReqInfo = pushRequestList.poll();
        assertEquals(2, pushReqInfo.req.notifyList.size());
        assertEquals("key1", new String(pushReqInfo.req.notifyList.get(0).key));
        assertEquals("key2", new String(pushReqInfo.req.notifyList.get(1).key));

        this.rpcResult = null;
        put("key1", "value1_2");
        put("key2", "value2_2");
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        assertEquals(0, pushRequestList.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSync_exAndRetry(boolean useException) {
        // retry interval new long[]{1, 1000}) in setup method
        manager.sync(kv, dtc1, false, keys("key1", "key2"), new long[]{0, 0});
        manager.sync(kv, dtc2, false, keys("key1"), new long[]{0});
        manager.dispatch();
        mockClientResponse();
        manager.dispatch();
        mockClientResponse();
        pushRequestList.clear();

        if (useException) {
            setRpcEx(new MockRuntimeException());
        } else {
            ReadPacket<WatchNotifyRespCallback> r = new ReadPacket<>();
            r.bizCode = KvCodes.CLIENT_REQ_ERROR;
            setRpcResult(r);
        }

        put("key2", "value2");
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushRequestList.clear();

        TestUtil.plus(ts, 300, TimeUnit.MILLISECONDS);
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        PushReqInfo pushReqInfo = pushRequestList.poll();
        assertEquals("key2", new String(pushReqInfo.req.notifyList.get(0).key));

        put("key1", "value1");
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(dtc2, pushReqInfo.ci.channel);

        setRpcEx(null);
        put("key1", "value1_2");

        manager.dispatch();
        mockClientResponse();
        assertEquals(0, pushRequestList.size());

        TestUtil.plus(ts, 20, TimeUnit.MILLISECONDS);
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(dtc2, pushReqInfo.ci.channel);
        assertEquals("value1_2", new String(pushReqInfo.req.notifyList.get(0).value));

        TestUtil.plus1Hour(ts);
        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        pushReqInfo = pushRequestList.poll();
        assertEquals(dtc1, pushReqInfo.ci.channel);
        assertEquals(2, pushReqInfo.req.notifyList.size());
        assertEquals("key2", new String(pushReqInfo.req.notifyList.get(0).key));
        assertEquals("value2", new String(pushReqInfo.req.notifyList.get(0).value));
        assertEquals("key1", new String(pushReqInfo.req.notifyList.get(1).key));
        assertEquals("value1_2", new String(pushReqInfo.req.notifyList.get(1).value));

        manager.dispatch();
        mockClientResponse();
        assertEquals(0, pushRequestList.size());
    }

    @Test
    public void testSync_requestSizeExceed() {
        kvConfig.watchMaxReqBytes = 1;

        manager.sync(kv, dtc1, false, keys("key1", "key2"), new long[]{0, 0});

        manager.dispatch();
        assertEquals(1, pushRequestList.size());
        assertEquals(1, pushRequestList.get(0).req.notifyList.size());
        pushRequestList.clear();

        mockClientResponse();
        assertEquals(1, pushRequestList.size());
        assertEquals(1, pushRequestList.get(0).req.notifyList.size());
        pushRequestList.clear();

        manager.dispatch();
        mockClientResponse();
        assertEquals(0, pushRequestList.size());
        assertTrue(manager.activeQueueHead.needNotify.isEmpty());
    }

    @Test
    public void testSync_batch() {
        kvConfig.watchMaxBatchSize = 2;

        manager.sync(kv, dtc1, false, keys("key1"), new long[]{0});
        manager.sync(kv, dtc2, false, keys("key1"), new long[]{0});
        manager.sync(kv, dtc3, false, keys("key1"), new long[]{0});
        manager.dispatch();
        mockClientResponse();
        assertEquals(2, pushRequestList.size());
        pushRequestList.clear();

        manager.dispatch();
        mockClientResponse();
        assertEquals(1, pushRequestList.size());
    }

    @Test
    public void testCleanTimeoutChannel() {
        manager.sync(kv, dtc1, false, keys("key1"), new long[]{0});
        manager.sync(kv, dtc2, false, keys("key1"), new long[]{0});

        manager.cleanTimeoutChannel(1_000_000);
        assertNotNull(manager.activeQueueHead);

        TestUtil.plus1Hour(ts);

        manager.cleanTimeoutChannel(1_000_000);
        assertNull(manager.activeQueueHead);

        manager.dispatch();
        mockClientResponse();

        assertEquals(0, pushRequestList.size());
    }

    @Test
    public void testUpdateWatchStatus() {
        manager.sync(kv, dtc1, false, keys("key1"), new long[]{0});

        TestUtil.plus1Hour(ts);
        assertEquals(1, manager.updateWatchStatus(dtc1));

        manager.cleanTimeoutChannel(1_000_000);
        assertNotNull(manager.activeQueueHead);
    }


    private void put(String key, String value) {
        kv.opContext.init(DtKV.BIZ_TYPE_PUT ,selfUuid, 0, ts.nanoTime, ts.nanoTime);
        kv.put(raftIndex++, ba(key), value.getBytes());
    }

    private void mkdir(String key) {
        kv.opContext.init(DtKV.BIZ_TYPE_MKDIR, selfUuid, 0, ts.nanoTime, ts.nanoTime);
        kv.mkdir(raftIndex++, ba(key));
    }

    private void remove(String key) {
        kv.opContext.init(DtKV.BIZ_TYPE_REMOVE, selfUuid, 0, ts.nanoTime, ts.nanoTime);
        kv.remove(raftIndex++, ba(key));
    }

    private ByteArray ba(String s) {
        return new ByteArray(s.getBytes());
    }

    private ByteArray[] keys(String... k) {
        ByteArray[] keys = new ByteArray[k.length];
        for (int i = 0; i < k.length; i++) {
            keys[i] = new ByteArray(k[i].getBytes());
        }
        return keys;
    }

    private void setRpcEx(Throwable ex) {
        rpcEx = ex;
        rpcResult = null;
    }

    private ReadPacket<WatchNotifyRespCallback> createRpcResult(int... results) {
        WatchNotifyRespCallback resp = new WatchNotifyRespCallback(results.length);
        System.arraycopy(results, 0, resp.results, 0, results.length);
        ReadPacket<WatchNotifyRespCallback> r = new ReadPacket<>();
        r.setBody(resp);
        return r;
    }

    private void setRpcResult(int... results) {
        rpcEx = null;
        rpcResult = createRpcResult(results);
    }

    private void setRpcResult(ReadPacket<WatchNotifyRespCallback> r) {
        rpcEx = null;
        rpcResult = r;
    }


    private static class MockDtChannel implements DtChannel {

        SocketChannel channel;

        @Override
        public SocketChannel getChannel() {
            return channel;
        }

        @Override
        public SocketAddress getRemoteAddr() {
            return null;
        }

        @Override
        public SocketAddress getLocalAddr() {
            return null;
        }

        @Override
        public Peer getPeer() {
            return null;
        }

        @Override
        public long getLastActiveTimeNanos() {
            return 0;
        }

        @Override
        public NioNet getOwner() {
            return null;
        }

        @Override
        public UUID getRemoteUuid(){
            return null;
        }

        @Override
        public int getLocalPort() {
            return 0;
        }
    }
}

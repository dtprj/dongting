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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;

/**
 * @author huangli
 */
@SuppressWarnings({"resource", "StatementWithEmptyBody", "SameParameterValue"})
public class MemRaftLogTest {

    private RaftStatusImpl raftStatus;
    private LogItem[] items;
    private RaftGroupConfig config;

    private MemRaftLog setup(int maxItems, int itemsSize) {
        items = new LogItem[itemsSize];
        for (int i = 0; i < itemsSize; i++) {
            items[i] = new LogItem(null);
            items[i].setIndex(i + 1);
        }

        raftStatus = new RaftStatusImpl();
        config = new RaftGroupConfig(0, "1", "1");
        config.setTs(raftStatus.getTs());
        config.setRaftStatus(raftStatus);

        return new MemRaftLog(config, maxItems);
    }

//    @Test
//    public void testAppend() throws Exception {
//        MemRaftLog log = setup(2, 5);
//
//        log.append(Collections.singletonList(items[0]));
//        assertEquals(1, log.getLogs().size());
//        log.append(Collections.singletonList(items[1]));
//        assertEquals(2, log.getLogs().size());
//        log.append(Collections.singletonList(items[2]));
//        assertEquals(3, log.getLogs().size());
//        log.append(Collections.singletonList(items[3]));
//        assertEquals(4, log.getLogs().size());
//
//        raftStatus.setLastApplied(3);
//        log.append(Collections.singletonList(items[4]));
//        assertEquals(3, log.getLogs().size());
//
//        assertTrue(items[0].release());
//        assertTrue(items[1].release());
//        assertFalse(items[2].release());
//        assertFalse(items[3].release());
//        assertFalse(items[4].release());
//    }
//
//    @Test
//    public void testClose() throws Exception {
//        MemRaftLog log = setup(2, 2);
//
//        log.append(Collections.singletonList(items[0]));
//        log.append(Collections.singletonList(items[1]));
//
//        log.close();
//
//        assertEquals(0, log.getLogs().size());
//        assertTrue(items[0].release());
//        assertTrue(items[1].release());
//    }
//
//    @Test
//    public void testDeleteByIndex1() throws Exception {
//        MemRaftLog log = setup(2, 2);
//
//        log.append(Collections.singletonList(items[0]));
//        log.append(Collections.singletonList(items[1]));
//
//        log.markTruncateByIndex(2, 0);
//        assertEquals(2, log.getLogs().size());
//
//        while (!raftStatus.getTs().refresh(1)) ;
//        log.doDelete();
//        // not apply, so not delete
//        assertEquals(2, log.getLogs().size());
//    }
//
//    @Test
//    public void testDeleteByIndex2() throws Exception {
//        MemRaftLog log = setup(2, 2);
//
//        log.append(Collections.singletonList(items[0]));
//        log.append(Collections.singletonList(items[1]));
//
//        raftStatus.setLastApplied(1);
//
//        log.markTruncateByIndex(2, 0);
//        assertEquals(2, log.getLogs().size());
//
//        while (!raftStatus.getTs().refresh(1)) ;
//        log.doDelete();
//        // delete from applyIndex + 1, so not delete
//        assertEquals(2, log.getLogs().size());
//    }
//
//    @Test
//    public void testDeleteByIndex3() throws Exception {
//        MemRaftLog log = setup(2, 2);
//
//        log.append(Collections.singletonList(items[0]));
//        log.append(Collections.singletonList(items[1]));
//
//        raftStatus.setLastApplied(2);
//
//        log.markTruncateByIndex(2, 1);
//        assertEquals(2, log.getLogs().size());
//
//        Timestamp ts = raftStatus.getTs();
//        long t = ts.getWallClockMillis();
//        while (!ts.refresh(1)) ;
//        if (ts.getWallClockMillis() == t + 1) {
//            log.doDelete();
//            // delay 1ms, not delete if timestamp == t + 1
//            assertEquals(2, log.getLogs().size());
//            while (!ts.refresh(1)) ;
//
//        }
//        log.doDelete();
//        assertEquals(1, log.getLogs().size());
//    }
//
//    @Test
//    public void testDeleteByIndex4() throws Exception {
//        MemRaftLog log = setup(2, 2);
//
//        log.append(Collections.singletonList(items[0]));
//        log.append(Collections.singletonList(items[1]));
//
//        raftStatus.setLastApplied(2);
//
//        log.markTruncateByIndex(2, 1000);
//        assertEquals(2, log.getLogs().size());
//
//        Timestamp ts = raftStatus.getTs();
//        while (!ts.refresh(1)) ;
//        log.doDelete();
//        // delay 1000ms, so not deleted
//        assertEquals(2, log.getLogs().size());
//
//        log.markTruncateByIndex(2, 0);
//        while (!ts.refresh(1)) ;
//        log.doDelete();
//        assertEquals(1, log.getLogs().size());
//    }
//
//    @Test
//    public void testDeleteByTime1() throws Exception {
//        MemRaftLog log = setup(2, 2);
//        Timestamp ts = raftStatus.getTs();
//
//        items[0].setTimestamp(ts.getWallClockMillis());
//        log.append(Collections.singletonList(items[0]));
//        items[1].setTimestamp(ts.getWallClockMillis() + 1);
//        log.append(Collections.singletonList(items[1]));
//
//        log.markTruncateByTimestamp(ts.getWallClockMillis() + 1, 0);
//        assertEquals(2, log.getLogs().size());
//
//        while (!raftStatus.getTs().refresh(1)) ;
//        log.doDelete();
//        // not apply, so not delete
//        assertEquals(2, log.getLogs().size());
//    }
//
//    @Test
//    public void testDeleteByTime2() throws Exception {
//        MemRaftLog log = setup(2, 2);
//        Timestamp ts = raftStatus.getTs();
//
//        items[0].setTimestamp(ts.getWallClockMillis());
//        log.append(Collections.singletonList(items[0]));
//        items[1].setTimestamp(ts.getWallClockMillis() + 1);
//        log.append(Collections.singletonList(items[1]));
//
//        raftStatus.setLastApplied(2);
//
//        log.markTruncateByTimestamp(ts.getWallClockMillis() + 1, 0);
//        assertEquals(2, log.getLogs().size());
//
//        while (!raftStatus.getTs().refresh(1)) ;
//        log.doDelete();
//        assertEquals(1, log.getLogs().size());
//    }
//
//    @Test
//    public void testTryFindMatchPos1() throws Exception {
//        MemRaftLog log = setup(2, 1);
//        assertNull(log.tryFindMatchPos(1, 1, null).get());
//
//        items[0].setIndex(100);
//        items[0].setTerm(10);
//        log.append(Collections.singletonList(items[0]));
//
//        assertNull(log.tryFindMatchPos(1, 1, null).get());
//        assertNull(log.tryFindMatchPos(9, 100, null).get());
//        assertNull(log.tryFindMatchPos(10, 99, null).get());
//        assertNull(log.tryFindMatchPos(11, 100, null).get());
//
//        assertEquals(new Pair<>(10, 100L),
//                log.tryFindMatchPos(10, 100, null).get());
//        assertEquals(new Pair<>(10, 100L),
//                log.tryFindMatchPos(10, 101, null).get());
//        assertEquals(new Pair<>(10, 100L),
//                log.tryFindMatchPos(11, 101, null).get());
//
//        log.getLogs().get(0).deleteTimestamp = 100;
//        assertNull(log.tryFindMatchPos(10, 100, null).get());
//    }
//
//    @Test
//    public void testTryFindMatchPos2() throws Exception {
//        MemRaftLog log = setup(5, 5);
//        for (int i = 0; i < items.length; i++) {
//            items[i].setIndex(100 + i);
//            items[i].setTerm(10);
//        }
//        log.append(Arrays.asList(items));
//
//        assertNull(log.tryFindMatchPos(1, 1, null).get());
//        assertNull(log.tryFindMatchPos(9, 100, null).get());
//        assertNull(log.tryFindMatchPos(10, 99, null).get());
//        assertNull(log.tryFindMatchPos(11, 100, null).get());
//
//        for (int i = 0; i < items.length; i++) {
//            assertEquals(new Pair<>(10, 100L + i),
//                    log.tryFindMatchPos(10, 100 + i, null).get());
//        }
//
//        assertEquals(new Pair<>(10, 104L),
//                log.tryFindMatchPos(10, 105, null).get());
//        assertEquals(new Pair<>(10, 104L),
//                log.tryFindMatchPos(11, 105, null).get());
//        assertEquals(new Pair<>(10, 103L),
//                log.tryFindMatchPos(11, 104, null).get());
//
//        log.getLogs().get(0).deleteTimestamp = 100;
//        assertNull(log.tryFindMatchPos(10, 100, null).get());
//    }
//
//    @Test
//    public void testTryFindMatchPos3() throws Exception {
//        MemRaftLog log = setup(5, 5);
//        for (int i = 0; i < items.length; i++) {
//            items[i].setIndex(100 + i);
//            items[i].setTerm(10 + i);
//        }
//        log.append(Arrays.asList(items));
//
//        assertNull(log.tryFindMatchPos(1, 1, null).get());
//        assertNull(log.tryFindMatchPos(9, 100, null).get());
//        assertNull(log.tryFindMatchPos(10, 99, null).get());
//        assertNull(log.tryFindMatchPos(11, 100, null).get());
//
//        for (int i = 0; i < items.length; i++) {
//            assertEquals(new Pair<>(10 + i, 100L + i),
//                    log.tryFindMatchPos(10 + i, 100 + i, null).get());
//        }
//
//        assertEquals(new Pair<>(12, 102L),
//                log.tryFindMatchPos(12, 103, null).get());
//        assertEquals(new Pair<>(10, 100L),
//                log.tryFindMatchPos(12, 101, null).get());
//        assertEquals(new Pair<>(13, 103L),
//                log.tryFindMatchPos(13, 200, null).get());
//        assertEquals(new Pair<>(12, 102L),
//                log.tryFindMatchPos(200, 103, null).get());
//    }
//
//    @Test
//    public void testIterator1() throws Exception {
//        MemRaftLog log = setup(1, 1);
//        {
//            RaftLog.LogIterator it = log.openIterator(() -> true);
//            assertThrows(CancellationException.class, () -> it.next(0, 1, 1).get());
//        }
//        {
//            RaftLog.LogIterator it = log.openIterator(() -> false);
//            assertEquals(0, it.next(0, 1, 1).get().size());
//        }
//        {
//            RaftLog.LogIterator it = log.openIterator(() -> false);
//            log.close();
//            assertThrows(CancellationException.class, () -> it.next(0, 1, 1).get());
//        }
//    }
//
//    private void initEncoder() {
//        config.setCodecFactory(new MockRaftCodecFactory() {
//            @Override
//            public Encoder<?> createBodyEncoder(int bizType) {
//                return new Encoder<>() {
//                    @Override
//                    public boolean encode(EncodeContext context, ByteBuffer buffer, Object data) {
//                        return false;
//                    }
//
//                    @Override
//                    public int actualSize(Object data) {
//                        return 100;
//                    }
//                };
//            }
//        });
//    }
//
//    @Test
//    public void testIterator2() throws Exception {
//        MemRaftLog log = setup(5, 5);
//        initEncoder();
//        log.append(Arrays.asList(items));
//        RaftLog.LogIterator it = log.openIterator(() -> false);
//        List<LogItem> list = it.next(1, 3, 10000).get();
//        assertEquals(3, list.size());
//        list = it.next(4, 3, 10000).get();
//        assertEquals(2, list.size());
//        log.close();
//        for (LogItem li : items) {
//            assertFalse(li.release());
//            assertTrue(li.release());
//        }
//        it.close();
//    }
//
//    @Test
//    public void testIterator3() throws Exception {
//        MemRaftLog log = setup(5, 5);
//        initEncoder();
//        log.append(Arrays.asList(items));
//        RaftLog.LogIterator it = log.openIterator(() -> false);
//        List<LogItem> list = it.next(1, 200, 350).get();
//        assertEquals(3, list.size());
//        list = it.next(4, 200, 350).get();
//        assertEquals(2, list.size());
//        log.close();
//        for (LogItem li : items) {
//            assertFalse(li.release());
//            assertTrue(li.release());
//        }
//        it.close();
//    }
//
//    @Test
//    public void testIterator4() throws Exception {
//        MemRaftLog log = setup(5, 5);
//        initEncoder();
//        log.append(Arrays.asList(items));
//        RaftLog.LogIterator it = log.openIterator(() -> false);
//        assertThrows(ExecutionException.class, () -> it.next(6, 200, 350).get());
//        it.close();
//    }

}

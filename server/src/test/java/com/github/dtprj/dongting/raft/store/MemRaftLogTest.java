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

import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
@SuppressWarnings({"resource", "StatementWithEmptyBody", "SameParameterValue"})
public class MemRaftLogTest {

    private RaftStatusImpl raftStatus;
    private LogItem[] items;

    private MemRaftLog setup(int maxItems, int itemsSize) {
        items = new LogItem[itemsSize];
        for (int i = 0; i < itemsSize; i++) {
            items[i] = new LogItem(null);
            items[i].setIndex(i + 1);
        }

        raftStatus = new RaftStatusImpl();
        RaftGroupConfigEx c = new RaftGroupConfigEx(0, "1", "1");
        c.setTs(raftStatus.getTs());
        c.setRaftStatus(raftStatus);

        return new MemRaftLog(c, maxItems);
    }

    @Test
    public void testAppend() throws Exception {
        MemRaftLog log = setup(2, 5);

        log.append(Collections.singletonList(items[0]));
        assertEquals(1, log.getLogs().size());
        log.append(Collections.singletonList(items[1]));
        assertEquals(2, log.getLogs().size());
        log.append(Collections.singletonList(items[2]));
        assertEquals(3, log.getLogs().size());
        log.append(Collections.singletonList(items[3]));
        assertEquals(4, log.getLogs().size());

        raftStatus.setLastApplied(3);
        log.append(Collections.singletonList(items[4]));
        assertEquals(3, log.getLogs().size());

        assertTrue(items[0].release());
        assertTrue(items[1].release());
        assertFalse(items[2].release());
        assertFalse(items[3].release());
        assertFalse(items[4].release());
    }

    @Test
    public void testClose() throws Exception {
        MemRaftLog log = setup(2, 2);

        log.append(Collections.singletonList(items[0]));
        log.append(Collections.singletonList(items[1]));

        log.close();

        assertEquals(0, log.getLogs().size());
        assertTrue(items[0].release());
        assertTrue(items[1].release());
    }

    @Test
    public void testDeleteByIndex1() throws Exception {
        MemRaftLog log = setup(2, 2);

        log.append(Collections.singletonList(items[0]));
        log.append(Collections.singletonList(items[1]));

        log.markTruncateByIndex(2, 0);
        assertEquals(2, log.getLogs().size());

        while (!raftStatus.getTs().refresh(1));
        log.doDelete();
        // not apply, so not delete
        assertEquals(2, log.getLogs().size());
    }

    @Test
    public void testDeleteByIndex2() throws Exception {
        MemRaftLog log = setup(2, 2);

        log.append(Collections.singletonList(items[0]));
        log.append(Collections.singletonList(items[1]));

        raftStatus.setLastApplied(1);

        log.markTruncateByIndex(2, 0);
        assertEquals(2, log.getLogs().size());

        while (!raftStatus.getTs().refresh(1)) ;
        log.doDelete();
        // delete from applyIndex + 1, so not delete
        assertEquals(2, log.getLogs().size());
    }

    @Test
    public void testDeleteByIndex3() throws Exception {
        MemRaftLog log = setup(2, 2);

        log.append(Collections.singletonList(items[0]));
        log.append(Collections.singletonList(items[1]));

        raftStatus.setLastApplied(2);

        log.markTruncateByIndex(2, 1);
        assertEquals(2, log.getLogs().size());

        Timestamp ts = raftStatus.getTs();
        long t = ts.getWallClockMillis();
        while (!ts.refresh(1)) ;
        if (ts.getWallClockMillis() == t + 1) {
            log.doDelete();
            // delay 1ms, not delete if timestamp == t + 1
            assertEquals(2, log.getLogs().size());
            while (!ts.refresh(1)) ;

        }
        log.doDelete();
        assertEquals(1, log.getLogs().size());
    }

    @Test
    public void testDeleteByIndex4() throws Exception {
        MemRaftLog log = setup(2, 2);

        log.append(Collections.singletonList(items[0]));
        log.append(Collections.singletonList(items[1]));

        raftStatus.setLastApplied(2);

        log.markTruncateByIndex(2, 1000);
        assertEquals(2, log.getLogs().size());

        Timestamp ts = raftStatus.getTs();
        while (!ts.refresh(1)) ;
        log.doDelete();
        // delay 1000ms, so not deleted
        assertEquals(2, log.getLogs().size());

        log.markTruncateByIndex(2, 0);
        while (!ts.refresh(1)) ;
        log.doDelete();
        assertEquals(1, log.getLogs().size());
    }

    @Test
    public void testDeleteByTime1() throws Exception {
        MemRaftLog log = setup(2, 2);
        Timestamp ts = raftStatus.getTs();

        items[0].setTimestamp(ts.getWallClockMillis());
        log.append(Collections.singletonList(items[0]));
        items[1].setTimestamp(ts.getWallClockMillis() + 1);
        log.append(Collections.singletonList(items[1]));

        log.markTruncateByTimestamp(ts.getWallClockMillis() + 1, 0);
        assertEquals(2, log.getLogs().size());

        while (!raftStatus.getTs().refresh(1)) ;
        log.doDelete();
        // not apply, so not delete
        assertEquals(2, log.getLogs().size());
    }

    @Test
    public void testDeleteByTime2() throws Exception {
        MemRaftLog log = setup(2, 2);
        Timestamp ts = raftStatus.getTs();

        items[0].setTimestamp(ts.getWallClockMillis());
        log.append(Collections.singletonList(items[0]));
        items[1].setTimestamp(ts.getWallClockMillis() + 1);
        log.append(Collections.singletonList(items[1]));

        raftStatus.setLastApplied(2);

        log.markTruncateByTimestamp(ts.getWallClockMillis() + 1, 0);
        assertEquals(2, log.getLogs().size());

        while (!raftStatus.getTs().refresh(1)) ;
        log.doDelete();
        assertEquals(1, log.getLogs().size());
    }
}

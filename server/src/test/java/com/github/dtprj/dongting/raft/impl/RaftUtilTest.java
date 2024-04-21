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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.FiberTestUtil;
import com.github.dtprj.dongting.net.HostPort;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
public class RaftUtilTest {
    @Test
    public void testUpdateLease() {
        RaftStatusImpl rs = new RaftStatusImpl(new Timestamp());
        RaftNodeEx n1 = new RaftNodeEx(1, new HostPort("127.0.0.1", 10001), false, null);
        RaftNodeEx n2 = new RaftNodeEx(2, new HostPort("127.0.0.1", 10002), false, null);
        RaftNodeEx n3 = new RaftNodeEx(3, new HostPort("127.0.0.1", 10003), false, null);
        RaftNodeEx n4 = new RaftNodeEx(4, new HostPort("127.0.0.1", 10004), false, null);
        RaftNodeEx n5 = new RaftNodeEx(4, new HostPort("127.0.0.1", 10005), false, null);
        RaftMember m1 = new RaftMember(n1, FiberTestUtil.FIBER_GROUP);
        RaftMember m2 = new RaftMember(n2, FiberTestUtil.FIBER_GROUP);
        RaftMember m3 = new RaftMember(n3, FiberTestUtil.FIBER_GROUP);
        RaftMember m4 = new RaftMember(n4, FiberTestUtil.FIBER_GROUP);
        RaftMember m5 = new RaftMember(n5, FiberTestUtil.FIBER_GROUP);
        m1.setLastConfirmReqNanos(-100);
        m2.setLastConfirmReqNanos(-200);
        m3.setLastConfirmReqNanos(200);
        m4.setLastConfirmReqNanos(300);
        m5.setLastConfirmReqNanos(100);

        rs.setPreparedMembers(Collections.emptyList());

        rs.setMembers(List.of(m1));
        rs.setElectQuorum(RaftUtil.getElectQuorum(rs.getMembers().size()));
        RaftUtil.updateLease(rs);
        assertEquals(-100, rs.getLeaseStartNanos());

        rs.setMembers(List.of(m1, m2));
        rs.setElectQuorum(RaftUtil.getElectQuorum(rs.getMembers().size()));
        RaftUtil.updateLease(rs);
        assertEquals(-200, rs.getLeaseStartNanos());

        rs.setMembers(List.of(m1, m2, m3));
        rs.setElectQuorum(RaftUtil.getElectQuorum(rs.getMembers().size()));
        RaftUtil.updateLease(rs);
        assertEquals(-100, rs.getLeaseStartNanos());

        rs.setMembers(List.of(m1, m2, m3, m4));
        rs.setElectQuorum(RaftUtil.getElectQuorum(rs.getMembers().size()));
        RaftUtil.updateLease(rs);
        assertEquals(-100, rs.getLeaseStartNanos());

        rs.setMembers(List.of(m1, m2, m3, m4, m5));
        rs.setElectQuorum(RaftUtil.getElectQuorum(rs.getMembers().size()));
        RaftUtil.updateLease(rs);
        assertEquals(100, rs.getLeaseStartNanos());

        rs.setMembers(List.of(m1, m2, m3, m4, m5));
        rs.setElectQuorum(RaftUtil.getElectQuorum(rs.getMembers().size()));
        rs.setPreparedMembers(List.of(m1));
        RaftUtil.updateLease(rs);
        assertEquals(-100, rs.getLeaseStartNanos());

        rs.setMembers(List.of(m1, m2, m3, m4, m5));
        rs.setElectQuorum(RaftUtil.getElectQuorum(rs.getMembers().size()));
        rs.setPreparedMembers(List.of(m3));
        RaftUtil.updateLease(rs);
        assertEquals(100, rs.getLeaseStartNanos());
    }
}

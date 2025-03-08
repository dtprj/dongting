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

import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberGroup;

/**
 * @author huangli
 */
public class RaftMember {
    public final RaftNodeEx node;
    public final FiberCondition repDoneCondition;
    public boolean ready;
    public boolean pinging;

    public long lastConfirmReqNanos;

    // in raft paper: volatile state on leaders
    public long nextIndex;
    public long matchIndex;

    public long repCommitIndex;
    public long repCommitIndexAcked;

    public int replicateEpoch;
    public int nodeEpoch;
    public boolean installSnapshot;

    public RaftMember(RaftNodeEx node, FiberGroup fg) {
        this.node = node;
        this.repDoneCondition = fg.newCondition("repDone-" + node.nodeId);
    }

}

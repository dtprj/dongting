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
package com.github.dtprj.dongting.raft;

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.codec.SimpleEncodable;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author huangli
 */
public class QueryStatusResp extends PbCallback<QueryStatusResp> implements SimpleEncodable {
    // uint32 group_id = 1;
    // uint32 term = 2;
    // uint32 leader_id = 3;
    // fixed64 commit_index = 4;
    // fixed64 last_applied = 5;
    // fixed64 last_log_index = 6;
    // repeated fixed32 members = 7[packed = false];
    // repeated fixed32 observers = 8[packed = false];
    // repeated fixed32 prepared_members = 9[packed = false];
    // repeated fixed32 prepared_observers = 10[packed = false];
    private int groupId;
    private int term;
    private int leaderId;
    private long commitIndex;
    private long lastApplied;
    private long lastLogIndex;
    private Set<Integer> members = Collections.emptySet();
    private Set<Integer> observers = Collections.emptySet();
    private Set<Integer> preparedMembers = Collections.emptySet();
    private Set<Integer> preparedObservers = Collections.emptySet();

    public static final DecoderCallbackCreator<QueryStatusResp> DECODER = ctx -> ctx.toDecoderCallback(
            new QueryStatusResp());

    @Override
    public int actualSize() {
        return PbUtil.accurateUnsignedIntSize(1, groupId) +
                PbUtil.accurateUnsignedIntSize(2, term) +
                PbUtil.accurateUnsignedIntSize(3, leaderId) +
                PbUtil.accurateFix64Size(4, commitIndex) +
                PbUtil.accurateFix64Size(5, lastApplied) +
                PbUtil.accurateFix64Size(6, lastLogIndex) +
                PbUtil.accurateFix32Size(7, members) +
                PbUtil.accurateFix32Size(8, observers) +
                PbUtil.accurateFix32Size(9, preparedMembers) +
                PbUtil.accurateFix32Size(10, preparedObservers);
    }

    @Override
    public void encode(ByteBuffer buf) {
        PbUtil.writeUnsignedInt32(buf, 1, groupId);
        PbUtil.writeUnsignedInt32(buf, 2, term);
        PbUtil.writeUnsignedInt32(buf, 3, leaderId);
        PbUtil.writeFix64(buf, 4, commitIndex);
        PbUtil.writeFix64(buf, 5, lastApplied);
        PbUtil.writeFix64(buf, 6, lastLogIndex);
        PbUtil.writeFix32(buf, 7, members);
        PbUtil.writeFix32(buf, 8, observers);
        PbUtil.writeFix32(buf, 9, preparedMembers);
        PbUtil.writeFix32(buf, 10, preparedObservers);
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case 1:
                groupId = (int) value;
                break;
            case 2:
                term = (int) value;
                break;
            case 3:
                leaderId = (int) value;
                break;
        }
        return true;
    }

    @Override
    public boolean readFix32(int index, int value) {
        switch (index) {
            case 7:
                if (members == Collections.<Integer>emptySet()) {
                    members = new HashSet<>();
                }
                members.add(value);
                break;
            case 8:
                if (observers == Collections.<Integer>emptySet()) {
                    observers = new HashSet<>();
                }
                observers.add(value);
                break;
            case 9:
                if (preparedMembers == Collections.<Integer>emptySet()) {
                    preparedMembers = new HashSet<>();
                }
                preparedMembers.add(value);
                break;
            case 10:
                if (preparedObservers == Collections.<Integer>emptySet()) {
                    preparedObservers = new HashSet<>();
                }
                preparedObservers.add(value);
                break;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        switch (index) {
            case 4:
                commitIndex = value;
                break;
            case 5:
                lastApplied = value;
                break;
            case 6:
                lastLogIndex = value;
                break;
        }
        return true;
    }

    @Override
    public QueryStatusResp getResult() {
        return this;
    }

    public int getGroupId() {
        return groupId;
    }

    public int getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public Set<Integer> getMembers() {
        return members;
    }

    public void setMembers(Set<Integer> members) {
        this.members = members;
    }

    public Set<Integer> getObservers() {
        return observers;
    }

    public void setObservers(Set<Integer> observers) {
        this.observers = observers;
    }

    public Set<Integer> getPreparedMembers() {
        return preparedMembers;
    }

    public void setPreparedMembers(Set<Integer> preparedMembers) {
        this.preparedMembers = preparedMembers;
    }

    public Set<Integer> getPreparedObservers() {
        return preparedObservers;
    }

    public void setPreparedObservers(Set<Integer> preparedObservers) {
        this.preparedObservers = preparedObservers;
    }
}

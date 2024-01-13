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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.Collection;
import java.util.HashSet;
import java.util.function.BiConsumer;

/**
 * @author huangli
 */
public class VoteManager implements BiConsumer<EventType, Object> {

    private static final DtLog log = DtLogs.getLogger(VoteManager.class);
    private final LinearTaskRunner linearTaskRunner;
    private final NioClient client;
    private final RaftStatusImpl raftStatus;
    private final RaftServerConfig config;
    private final int groupId;
    private final StatusManager statusManager;

    private boolean voting;
    private HashSet<Integer> votes;
    private int votePendingCount;
    private int currentVoteId;

    public VoteManager(RaftServerConfig serverConfig, int groupId, RaftStatusImpl raftStatus,
                       NioClient client, LinearTaskRunner linearTaskRunner, StatusManager statusManager) {
        this.linearTaskRunner = linearTaskRunner;
        this.client = client;
        this.raftStatus = raftStatus;
        this.config = serverConfig;
        this.groupId = groupId;
        this.statusManager = statusManager;
    }

    @Override
    public void accept(EventType eventType, Object o) {
        if (eventType == EventType.cancelVote) {
            cancelVote();
        }
    }

    public void cancelVote() {
        if (voting) {
            log.info("cancel current voting: groupId={}, voteId={}", groupId, currentVoteId);
            voting = false;
            votes = null;
            currentVoteId++;
            votePendingCount = 0;
        }
    }

    private void initStatusForVoting(int count) {
        voting = true;
        currentVoteId++;
        votes = new HashSet<>();
        votes.add(config.getNodeId());
        votePendingCount = count - 1;
    }

    private void descPending(int voteIdOfRequest) {
        if (voteIdOfRequest != currentVoteId) {
            return;
        }
        if (--votePendingCount == 0) {
            voting = false;
            votes = null;
        }
    }

    private int readyCount(Collection<RaftMember> list) {
        int count = 0;
        for (RaftMember member : list) {
            if (member.isReady()) {
                // include self
                count++;
            }
        }
        return count;
    }

}

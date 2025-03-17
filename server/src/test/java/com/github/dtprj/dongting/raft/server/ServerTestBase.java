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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtkv.server.DtKV;
import com.github.dtprj.dongting.dtkv.server.KvConfig;
import com.github.dtprj.dongting.dtkv.server.KvServerUtil;
import com.github.dtprj.dongting.fiber.Dispatcher;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.ImplAccessor;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;
import com.github.dtprj.dongting.raft.sm.StateMachine;
import com.github.dtprj.dongting.raft.store.DefaultRaftLog;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusFile;
import com.github.dtprj.dongting.raft.store.StatusManager;
import com.github.dtprj.dongting.raft.store.StoreAccessor;
import com.github.dtprj.dongting.raft.store.TestDir;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32C;

import static com.github.dtprj.dongting.util.Tick.tick;

/**
 * @author huangli
 */
public class ServerTestBase {

    protected int servicePortBase = 0;
    protected boolean startAfterCreate = true;
    protected int initTerm = 0;
    protected int initVoteFor = 0;
    protected long initCommitIndex = 0;
    protected boolean initSnapshot = false;
    protected int groupId = 1;

    protected int electTimeout = 25;
    protected int heartbeatInterval = 10;
    protected int rpcTimeout = 100;

    protected int idxCacheSize = 128;
    protected int idxFlushThreshold = 64;
    protected int idxItemsPerFile = 1024;
    protected int logFileSize = 512 * 1024;

    protected final Map<String, String> dirMap = new ConcurrentHashMap<>();

    @BeforeEach
    public void init() {
        dirMap.clear();
    }

    protected static class ServerInfo {
        public RaftServer raftServer;
        public int nodeId;
        public RaftGroupImpl group;
        public GroupComponents gc;
    }

    protected ServerInfo createServer(int nodeId, String servers, String nodeIdOfMembers,
                                      String nodeIdOfObservers) throws Exception {
        int replicatePort = 4000 + nodeId;
        RaftServerConfig serverConfig = new RaftServerConfig();
        serverConfig.servers = servers;
        serverConfig.nodeId = nodeId;
        serverConfig.replicatePort = replicatePort;
        if (servicePortBase > 0) {
            serverConfig.servicePort = servicePortBase + nodeId;
        }
        serverConfig.electTimeout = tick(electTimeout);
        serverConfig.heartbeatInterval = tick(heartbeatInterval);
        serverConfig.rpcTimeout = tick(rpcTimeout);

        RaftGroupConfig groupConfig = config(nodeId, groupId, nodeIdOfMembers, nodeIdOfObservers);

        DefaultRaftFactory raftFactory = createRaftFactory(nodeId);

        RaftServer raftServer = new RaftServer(serverConfig, Collections.singletonList(groupConfig),
                raftFactory, g -> {
            ImplAccessor.updateMemberManager(g.groupComponents.memberManager);
            ImplAccessor.updateVoteManager(g.groupComponents.voteManager);
        });
        if (servicePortBase > 0) {
            KvServerUtil.initKvServer(raftServer);
        }

        RaftGroupImpl g = (RaftGroupImpl) raftServer.getRaftGroup(groupId);
        GroupComponents gc = g.groupComponents;
        ImplAccessor.updateNodeManager(gc.nodeManager);

        if (initTerm > 0 || initVoteFor > 0 || initCommitIndex > 0 || initSnapshot) {
            File dir = new File(groupConfig.dataDir);
            //noinspection ResultOfMethodCallIgnored
            dir.mkdirs();
            File file = new File(dir, groupConfig.statusFile);
            ByteBuffer buf = ByteBuffer.allocate(StatusFile.FILE_LENGTH);
            Map<String, String> props = new HashMap<>();
            props.put(StatusManager.CURRENT_TERM, String.valueOf(initTerm));
            props.put(StatusManager.VOTED_FOR, String.valueOf(initVoteFor));
            props.put(StatusManager.COMMIT_INDEX, String.valueOf(initCommitIndex));
            props.put(StatusManager.INSTALL_SNAPSHOT, String.valueOf(initSnapshot));
            StatusFile.writeToBuffer(props, buf, new CRC32C());
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.write(buf.array());
            raf.close();
        }

        if (startAfterCreate) {
            raftServer.start();
        }

        ServerInfo serverInfo = new ServerInfo();
        serverInfo.raftServer = raftServer;
        serverInfo.nodeId = nodeId;
        serverInfo.group = g;
        serverInfo.gc = gc;

        return serverInfo;
    }

    private RaftGroupConfig config(int nodeId, int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
        RaftGroupConfig groupConfig = RaftGroupConfig.newInstance(groupId, nodeIdOfMembers, nodeIdOfObservers);
        String mapKey = groupId + "-" + nodeId;
        groupConfig.dataDir = dirMap.computeIfAbsent(mapKey, k -> TestDir.testDir("raftlog") + "-" + k);
        groupConfig.saveSnapshotWhenClose = false;
        config(groupConfig);
        return groupConfig;
    }

    protected void config(RaftGroupConfig config) {
    }

    private DefaultRaftFactory createRaftFactory(int nodeId) {
        return new DefaultRaftFactory() {
            @Override
            public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
                return new DtKV(groupConfig, new KvConfig());
            }

            @Override
            public Dispatcher createDispatcher(RaftServerConfig serverConfig, RaftGroupConfig groupConfig) {
                // we start multi nodes in same jvm, so use node id as part of dispatcher name
                return new Dispatcher("node-" + nodeId + "-" + groupConfig.groupId +
                        "-dispatcher", new DefaultPoolFactory(), groupConfig.perfCallback);
            }

            @Override
            public ExecutorService createBlockIoExecutor(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig) {
                return MockExecutors.ioExecutor();
            }

            @Override
            public RaftLog createRaftLog(RaftGroupConfigEx groupConfig, StatusManager statusManager, RaftCodecFactory codecFactory) {
                groupConfig.idxCacheSize = idxCacheSize;
                groupConfig.idxFlushThreshold = idxFlushThreshold;
                DefaultRaftLog raftLog = new DefaultRaftLog(groupConfig, statusManager, codecFactory);
                StoreAccessor.updateRaftLog(raftLog, idxItemsPerFile, logFileSize);
                return raftLog;
            }

            @Override
            public RaftGroupConfig createConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
                return config(nodeId, groupId, nodeIdOfMembers, nodeIdOfObservers);
            }
        };
    }

    protected void waitStart(ServerInfo si) throws Exception {
        si.raftServer.getAllGroupReadyFuture().get(5, TimeUnit.SECONDS);
    }

    protected void waitStop(ServerInfo si) {
        si.raftServer.stop(new DtTime(5, TimeUnit.SECONDS));
    }

    protected ServerInfo waitLeaderElectAndGetLeaderId(int groupId, ServerInfo... servers) {
        AtomicReference<ServerInfo> si = new AtomicReference<>();
        TestUtil.waitUtil(() -> {
            int leader = 0;
            for (ServerInfo server : servers) {
                if (server.raftServer.getRaftGroup(groupId).isLeader()) {
                    leader++;
                    si.set(server);
                }
            }
            if (leader > 1) {
                throw new RuntimeException("more than one leader");
            }
            return leader == 1;
        });
        return si.get();
    }
}

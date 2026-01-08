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
package com.github.dtprj.dongting.dist;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.impl.MembersInfo;
import com.github.dtprj.dongting.raft.impl.NodeManager;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author huangli
 */
public class SyncConfigProcessor extends ReqProcessor<Void> {

    private final RaftServer server;
    private final File serversFile;
    private final ReentrantLock lock = new ReentrantLock();

    public SyncConfigProcessor(RaftServer server, File serversFile) {
        this.server = server;
        this.serversFile = serversFile;
    }

    @Override
    public DecoderCallback<Void> createDecoderCallback(int command, DecodeContext context) {
        return DecoderCallback.VOID_DECODE_CALLBACK;
    }

    @Override
    public WritePacket process(ReadPacket<Void> packet, ReqContext reqContext) throws Exception {
        boolean servicePort = RaftProcessor.requestServicePort(reqContext, server.getServerConfig());
        if (!RaftProcessor.checkPort(servicePort, false, true)) {
            return RaftProcessor.createWrongPortRest(packet, reqContext);
        }
        boolean locked = lock.tryLock(5, TimeUnit.SECONDS);
        if (locked) {
            try {
                List<RaftNode> allNodes;
                List<Pair<Integer, MembersInfo>> groupsInfos;
                NodeManager nm = server.getNodeManager();
                nm.getLock().lock();
                try {
                    allNodes = nm.getAllNodes();
                    groupsInfos = server.getRaftGroups().values().stream()
                            .map(rg -> new Pair<>(rg.getGroupId(), rg.groupComponents.raftStatus.membersInfo))
                            .collect(Collectors.toList());
                } finally {
                    nm.getLock().unlock();
                }
                Collections.sort(allNodes);
                groupsInfos.sort(Comparator.comparingInt(Pair::getLeft));
                syncConfig(allNodes, groupsInfos);
                return new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            } finally {
                lock.unlock();
            }
        } else {
            EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
            p.msg = "sync lock timeout";
            return p;
        }
    }

    private void syncConfig(List<RaftNode> nodes, List<Pair<Integer, MembersInfo>> groupInfos) throws IOException {
        // generate config content
        StringBuilder sb = new StringBuilder();
        sb.append("servers = ").append(RaftNode.formatServers(nodes)).append("\n");
        sb.append("\n");
        for (Pair<Integer, MembersInfo> pair : groupInfos) {
            int groupId = pair.getLeft();
            MembersInfo info = pair.getRight();
            sb.append("group.").append(groupId).append(".nodeIdOfMembers = ");
            sb.append(formatNodeIds(info.nodeIdOfMembers));
            sb.append("\n");
            sb.append("group.").append(groupId).append(".nodeIdOfObservers = ");
            sb.append(formatNodeIds(info.nodeIdOfObservers));
            sb.append("\n");
        }
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);

        // write to temp file and fsync
        File tempFile = new File(serversFile.getParentFile(), serversFile.getName() + ".tmp");
        try (FileOutputStream fos = new FileOutputStream(tempFile);
             FileChannel channel = fos.getChannel()) {
            fos.write(content);
            channel.force(true);
        }

        // backup original file
        backupFile();

        // atomic move to replace original file
        Files.move(tempFile.toPath(), serversFile.toPath(),
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private String formatNodeIds(Set<Integer> nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty()) {
            return "";
        }
        return nodeIds.stream()
                .sorted()
                .map(String::valueOf)
                .collect(Collectors.joining(","));
    }

    private void backupFile() throws IOException {
        File parent = serversFile.getParentFile();
        String baseName = serversFile.getName();

        // find existing backup files
        File[] backups = parent.listFiles((dir, name) -> name.startsWith(baseName + ".bak."));

        if (backups != null && backups.length > 0) {
            // sort by last modified time (newest first)
            Arrays.sort(backups, Comparator.comparingLong(File::lastModified).reversed());
            // delete backups exceeding limit (keep 9, the new one will make it 10)
            for (int i = 9; i < backups.length; i++) {
                //noinspection ResultOfMethodCallIgnored
                backups[i].delete();
            }
        }

        // create new backup with timestamp
        String backupName = baseName + ".bak." + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        File backupFile = new File(parent, backupName);
        Files.copy(serversFile.toPath(), backupFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
}

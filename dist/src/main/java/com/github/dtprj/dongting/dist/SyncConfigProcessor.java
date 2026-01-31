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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.impl.MembersInfo;
import com.github.dtprj.dongting.raft.impl.NodeManager;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;

import java.io.File;
import java.io.FileInputStream;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author huangli
 */
public class SyncConfigProcessor extends ReqProcessor<Void> {

    private static final DtLog log = DtLogs.getLogger(SyncConfigProcessor.class);

    private final RaftServer server;
    private final File serversFile;
    private final ReentrantLock lock = new ReentrantLock();

    private final SequenceRunner sequenceRunner;

    public SyncConfigProcessor(RaftServer server, File serversFile) {
        this.server = server;
        this.serversFile = serversFile;
        this.sequenceRunner = new SequenceRunner(server.getSharedIoExecutor(), () -> {
            try {
                doSyncConfig();
            } catch (Throwable e) {
                throw new RaftException(e);
            }
        }, new int[]{1000, 10 * 1000, 30 * 1000, 60 * 1000});
    }

    @Override
    public DecoderCallback<Void> createDecoderCallback(int command, DecodeContext context) {
        return DecoderCallback.VOID_DECODE_CALLBACK;
    }

    @Override
    public WritePacket process(ReadPacket<Void> packet, ReqContext reqContext) {
        boolean servicePort = RaftProcessor.requestServicePort(reqContext, server.getServerConfig());
        if (!RaftProcessor.checkPort(servicePort, false, true)) {
            return RaftProcessor.createWrongPortRest(packet, reqContext);
        }
        sequenceRunner.submit(reqContext.getTimeout().rest(TimeUnit.MILLISECONDS), e -> {
            EmptyBodyRespPacket p;
            if (e == null) {
                p = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            } else {
                p = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
                p.msg = e.toString();
            }
            reqContext.writeRespInBizThreads(p);
        });
        return null;
    }

    public void syncConfigAsync() {
        try {
            sequenceRunner.submit();
        } catch (Exception e) {
            log.error("submit sync task error", e);
        }
    }

    private void doSyncConfig() throws Exception {
        boolean locked = lock.tryLock(5000, TimeUnit.MILLISECONDS);
        if (locked) {
            try {
                List<RaftNode> allNodes;
                Map<Integer, MembersInfo> groupInfoMap;
                NodeManager nm = server.getNodeManager();
                nm.getLock().lock();
                try {
                    allNodes = nm.getAllNodes();
                    groupInfoMap = server.getRaftGroups().values().stream()
                            .collect(Collectors.toMap(
                                    RaftGroupImpl::getGroupId,
                                    rg -> rg.groupComponents.raftStatus.membersInfo,
                                    (a, b) -> a,
                                    TreeMap::new
                            ));
                } finally {
                    nm.getLock().unlock();
                }
                Collections.sort(allNodes);
                doSyncConfig(allNodes, groupInfoMap);
            } finally {
                lock.unlock();
            }
        } else {
            throw new RaftException("sync lock timeout");
        }
    }

    private void doSyncConfig(List<RaftNode> nodes, Map<Integer, MembersInfo> groupInfoMap) throws IOException {
        // read old config to preserve other settings
        Properties oldProps = new Properties();
        try (FileInputStream fis = new FileInputStream(serversFile)) {
            oldProps.load(fis);
        }

        // track processed groups
        Set<Integer> processedGroupIds = new HashSet<>();

        // generate config content
        StringBuilder sb = new StringBuilder();
        sb.append("servers = ").append(RaftNode.formatServers(nodes)).append("\n");
        sb.append("\n");

        // iterate over all old properties
        for (String key : oldProps.stringPropertyNames()) {
            String value = oldProps.getProperty(key);

            if ("servers".equals(key)) {
                continue; // already written
            }

            if (key.startsWith(Bootstrap.GROUP_PREFIX)) {
                String rest = key.substring(Bootstrap.GROUP_PREFIX.length());
                int dotIndex = rest.indexOf('.');
                if (dotIndex > 0) {
                    String groupIdStr = rest.substring(0, dotIndex);
                    try {
                        int groupId = Integer.parseInt(groupIdStr);
                        MembersInfo info = groupInfoMap.get(groupId);
                        if (info == null) {
                            // deleted group, skip
                            continue;
                        }
                        String suffix = rest.substring(dotIndex + 1);
                        // check if this is nodeIdOfMembers or nodeIdOfObservers
                        if ("nodeIdOfMembers".equals(suffix) || "nodeIdOfObservers".equals(suffix)) {
                            if (processedGroupIds.add(groupId)) {
                                // write new nodeIdOfMembers and nodeIdOfObservers
                                writeMembersInfo(sb, groupId, info);
                            }
                        } else {
                            // preserve other group settings
                            sb.append(key).append(" = ").append(value).append("\n");
                        }
                    } catch (NumberFormatException e) {
                        // invalid group id, preserve as is
                        sb.append(key).append(" = ").append(value).append("\n");
                    }
                } else {
                    // invalid group key, preserve as is
                    sb.append(key).append(" = ").append(value).append("\n");
                }
            } else {
                // preserve non-group settings
                sb.append(key).append(" = ").append(value).append("\n");
            }
        }

        // write newly added groups
        for (Map.Entry<Integer, MembersInfo> entry : groupInfoMap.entrySet()) {
            int groupId = entry.getKey();
            if (processedGroupIds.add(groupId)) {
                MembersInfo info = entry.getValue();
                writeMembersInfo(sb, groupId, info);
            }
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

    private void writeMembersInfo(StringBuilder sb, int groupId, MembersInfo info) {
        sb.append(Bootstrap.GROUP_PREFIX).append(groupId).append(".nodeIdOfMembers = ");
        sb.append(formatNodeIds(info.nodeIdOfMembers));
        sb.append("\n");
        sb.append(Bootstrap.GROUP_PREFIX).append(groupId).append(".nodeIdOfObservers = ");
        sb.append(formatNodeIds(info.nodeIdOfObservers));
        sb.append("\n");
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

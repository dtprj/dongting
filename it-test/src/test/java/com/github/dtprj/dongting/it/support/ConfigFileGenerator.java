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
package com.github.dtprj.dongting.it.support;

import com.github.dtprj.dongting.dist.Bootstrap;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Generate configuration files for integration tests.
 *
 * @author huangli
 */
public class ConfigFileGenerator {

    public static class ProcessConfig {
        public final File configFile;
        public final File serversFile;
        public final File nodeDir;
        public final int nodeId;
        public final int replicatePort;
        public final int servicePort;

        ProcessConfig(int nodeId, File nodeDir, File configFile, File serversFile,
                      int replicatePort, int servicePort) {
            this.nodeDir = nodeDir;
            this.configFile = configFile;
            this.serversFile = serversFile;
            this.nodeId = nodeId;
            this.replicatePort = replicatePort;
            this.servicePort = servicePort;
        }
    }

    public static class ProcessConfigBuilder {
        private final int nodeId;
        private final Path baseDir;
        private final String serversStr;
        private final List<GroupDefinition> groups;

        private Long electTimeout;
        private Long rpcTimeout;
        private Long connectTimeout;
        private Long heartbeatInterval;
        private Long pingInterval;
        private Long watchTimeoutMillis;

        public ProcessConfigBuilder(int nodeId, Path baseDir, String serversStr, List<GroupDefinition> groups) {
            this.nodeId = nodeId;
            this.baseDir = baseDir;
            this.serversStr = serversStr;
            this.groups = groups;
        }

        public ProcessConfigBuilder electTimeout(Long electTimeout) {
            this.electTimeout = electTimeout;
            return this;
        }

        public ProcessConfigBuilder rpcTimeout(Long rpcTimeout) {
            this.rpcTimeout = rpcTimeout;
            return this;
        }

        public ProcessConfigBuilder connectTimeout(Long connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public ProcessConfigBuilder heartbeatInterval(Long heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public ProcessConfigBuilder pingInterval(Long pingInterval) {
            this.pingInterval = pingInterval;
            return this;
        }

        public ProcessConfigBuilder watchTimeoutMillis(Long watchTimeoutMillis) {
            this.watchTimeoutMillis = watchTimeoutMillis;
            return this;
        }

        public ProcessConfig build() throws IOException {
            return generateNodeConfig(this);
        }
    }

    public static class GroupDefinition {
        public final int groupId;
        public final String nodeIdOfMembers;
        public final String nodeIdOfObservers;

        public GroupDefinition(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
            this.groupId = groupId;
            this.nodeIdOfMembers = nodeIdOfMembers;
            this.nodeIdOfObservers = nodeIdOfObservers;
        }
    }

    public static class ClusterConfigBuilder {
        private final int[] memberIds;
        private final int groupId;
        private final Path baseDir;

        private int[] observerIds = new int[0];
        private Long electTimeout;
        private Long rpcTimeout;
        private Long connectTimeout;
        private Long heartbeatInterval;
        private Long pingInterval;
        private Long watchTimeoutMillis;

        public ClusterConfigBuilder(int[] memberIds, int groupId, Path baseDir) {
            this.memberIds = memberIds;
            this.groupId = groupId;
            this.baseDir = baseDir;
        }

        public ClusterConfigBuilder observerIds(int[] observerIds) {
            this.observerIds = observerIds;
            return this;
        }

        public ClusterConfigBuilder electTimeout(Long electTimeout) {
            this.electTimeout = electTimeout;
            return this;
        }

        public ClusterConfigBuilder rpcTimeout(Long rpcTimeout) {
            this.rpcTimeout = rpcTimeout;
            return this;
        }

        public ClusterConfigBuilder connectTimeout(Long connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public ClusterConfigBuilder heartbeatInterval(Long heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public ClusterConfigBuilder pingInterval(Long pingInterval) {
            this.pingInterval = pingInterval;
            return this;
        }

        public ClusterConfigBuilder watchTimeoutMillis(Long watchTimeoutMillis) {
            this.watchTimeoutMillis = watchTimeoutMillis;
            return this;
        }

        public List<ProcessConfig> build() throws IOException {
            List<ProcessConfig> result = new ArrayList<>();

            // Build servers string (members + observers)
            int[] allNodeIds = new int[memberIds.length + observerIds.length];
            System.arraycopy(memberIds, 0, allNodeIds, 0, memberIds.length);
            System.arraycopy(observerIds, 0, allNodeIds, memberIds.length, observerIds.length);
            String serversStr = ItUtil.formatReplicateServers(allNodeIds);

            // Build members string
            StringBuilder memberIdsStr = new StringBuilder();
            for (int nid : memberIds) {
                memberIdsStr.append(nid).append(",");
            }
            memberIdsStr.deleteCharAt(memberIdsStr.length() - 1);

            // Build observers string
            String observersStr = "";
            if (observerIds.length > 0) {
                StringBuilder sb = new StringBuilder();
                for (int oid : observerIds) {
                    sb.append(oid).append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                observersStr = sb.toString();
            }

            List<GroupDefinition> groupDefinitions = Collections.singletonList(
                    new GroupDefinition(groupId, memberIdsStr.toString(), observersStr));

            // Generate config for each member
            for (int nid : memberIds) {
                result.add(createProcessConfig(nid, serversStr, groupDefinitions));
            }

            // Generate config for each observer
            for (int oid : observerIds) {
                result.add(createProcessConfig(oid, serversStr, groupDefinitions));
            }

            return result;
        }

        private ProcessConfig createProcessConfig(int nid, String serversStr,
                                                  List<GroupDefinition> groupDefinitions) throws IOException {
            return new ProcessConfigBuilder(nid, baseDir, serversStr, groupDefinitions)
                    .electTimeout(electTimeout)
                    .rpcTimeout(rpcTimeout)
                    .connectTimeout(connectTimeout)
                    .heartbeatInterval(heartbeatInterval)
                    .pingInterval(pingInterval)
                    .watchTimeoutMillis(watchTimeoutMillis)
                    .build();
        }
    }

    /**
     * Generate configuration files for a single node
     */
    private static ProcessConfig generateNodeConfig(ProcessConfigBuilder builder) throws IOException {
        int replicatePort = ItUtil.replicatePort(builder.nodeId);
        int servicePort = ItUtil.servicePort(builder.nodeId);
        // Create node directory
        File nodeDir = builder.baseDir.resolve("node" + builder.nodeId).toFile();
        if (!nodeDir.exists() && !nodeDir.mkdirs()) {
            throw new IOException("Failed to create node directory: " + nodeDir);
        }

        File dataDir = new File(nodeDir, "data");

        // Create config.properties
        File configFile = new File(nodeDir, "config.properties");
        Properties configProps = new Properties();
        configProps.setProperty("nodeId", String.valueOf(builder.nodeId));
        configProps.setProperty("replicatePort", String.valueOf(replicatePort));
        configProps.setProperty("servicePort", String.valueOf(servicePort));
        configProps.setProperty("dataDir", dataDir.getAbsolutePath());

        // Add custom timeout settings if provided
        if (builder.electTimeout != null) {
            configProps.setProperty("electTimeout", String.valueOf(builder.electTimeout));
        }
        if (builder.rpcTimeout != null) {
            configProps.setProperty("rpcTimeout", String.valueOf(builder.rpcTimeout));
        }
        if (builder.connectTimeout != null) {
            configProps.setProperty("connectTimeout", String.valueOf(builder.connectTimeout));
        }
        if (builder.heartbeatInterval != null) {
            configProps.setProperty("heartbeatInterval", String.valueOf(builder.heartbeatInterval));
        }
        if (builder.pingInterval != null) {
            configProps.setProperty("pingInterval", String.valueOf(builder.pingInterval));
        }

        writeConfigFile(configProps, configFile);

        // Create servers.properties
        File serversFile = new File(nodeDir, "servers.properties");
        Properties serversProps = generateServersProperties(builder, builder.groups);
        writeConfigFile(serversProps, serversFile);

        return new ProcessConfig(builder.nodeId, nodeDir, configFile, serversFile, replicatePort, servicePort);
    }

    private static Properties generateServersProperties(ProcessConfigBuilder builder, List<GroupDefinition> groups) {
        Properties serversProps = new Properties();
        serversProps.setProperty("servers", builder.serversStr);
        RaftGroupConfigEx protoType = new RaftGroupConfigEx(0, "", "");
        for (GroupDefinition group : groups) {
            serversProps.setProperty(Bootstrap.GROUP_PREFIX + group.groupId + ".nodeIdOfMembers", group.nodeIdOfMembers);
            serversProps.setProperty(Bootstrap.GROUP_PREFIX + group.groupId + ".nodeIdOfObservers", group.nodeIdOfObservers);
            serversProps.setProperty(Bootstrap.GROUP_PREFIX + group.groupId + ".idxItemsPerFile", String.valueOf(protoType.idxItemsPerFile / 64));
            serversProps.setProperty(Bootstrap.GROUP_PREFIX + group.groupId + ".idxCacheSize", String.valueOf(protoType.idxCacheSize / 64));
            serversProps.setProperty(Bootstrap.GROUP_PREFIX + group.groupId + ".idxFlushThreshold", String.valueOf(protoType.idxFlushThreshold / 64));
            serversProps.setProperty(Bootstrap.GROUP_PREFIX + group.groupId + ".logFileSize", String.valueOf(protoType.logFileSize / 64));
            if (builder.watchTimeoutMillis != null) {
                serversProps.setProperty("group." + group.groupId + ".watchTimeoutMillis", String.valueOf(builder.watchTimeoutMillis));
            }
        }
        return serversProps;
    }

    /**
     * Write properties to file with comments
     */
    public static void writeConfigFile(Properties props, File file) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            props.store(writer, "Generated by integration test");
        }
    }


}

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
        public final Long electTimeout;
        public final Long rpcTimeout;
        public final Long connectTimeout;
        public final Long heartbeatInterval;
        public final Long pingInterval;

        public ProcessConfig(File nodeDir, File configFile, File serversFile,
                             int nodeId, int replicatePort, int servicePort,
                             Long electTimeout, Long rpcTimeout, Long connectTimeout,
                             Long heartbeatInterval, Long pingInterval) {
            this.nodeDir = nodeDir;
            this.configFile = configFile;
            this.serversFile = serversFile;
            this.nodeId = nodeId;
            this.replicatePort = replicatePort;
            this.servicePort = servicePort;
            this.electTimeout = electTimeout;
            this.rpcTimeout = rpcTimeout;
            this.connectTimeout = connectTimeout;
            this.heartbeatInterval = heartbeatInterval;
            this.pingInterval = pingInterval;
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

    /**
     * Generate configuration files for a cluster with specified node count
     */
    public static List<ProcessConfig> generateClusterConfig(int[] nodeIds, int groupId, Path baseDir) throws IOException {
        return generateClusterConfig(nodeIds, groupId, baseDir, null, null, null, null, null);
    }

    /**
     * Generate configuration files for a cluster with custom timeout settings
     */
    public static List<ProcessConfig> generateClusterConfig(int[] nodeIds, int groupId, Path baseDir,
                                                           Long electTimeout, Long rpcTimeout,
                                                           Long connectTimeout, Long heartbeatInterval,
                                                           Long pingInterval) throws IOException {
        List<ProcessConfig> result = new ArrayList<>();

        // Build servers string
        String serversStr = ItUtil.formatReplicateServers(nodeIds);

        // Build members string
        StringBuilder ids = new StringBuilder();
        for (int nid : nodeIds) {
            ids.append(nid).append(",");
        }
        ids.deleteCharAt(ids.length() - 1);
        List<GroupDefinition> groupDefinitions = Collections.singletonList(
                new GroupDefinition(groupId, ids.toString(), ""));

        // Generate config for each node
        for (int nid : nodeIds) {
            ProcessConfig files = generateNodeConfig(nid, baseDir, serversStr, groupDefinitions,
                    electTimeout, rpcTimeout, connectTimeout, heartbeatInterval, pingInterval);
            result.add(files);
        }

        return result;
    }

    /**
     * Generate configuration files for a single node
     */
    public static ProcessConfig generateNodeConfig(int nodeId, Path baseDir, String serversStr,
                                                   List<GroupDefinition> groups) throws IOException {
        return generateNodeConfig(nodeId, baseDir, serversStr, groups,
                null, null, null, null, null);
    }

    /**
     * Generate configuration files for a single node with custom timeout settings
     */
    public static ProcessConfig generateNodeConfig(int nodeId, Path baseDir, String serversStr,
                                                   List<GroupDefinition> groups,
                                                   Long electTimeout, Long rpcTimeout,
                                                   Long connectTimeout, Long heartbeatInterval,
                                                   Long pingInterval) throws IOException {
        int replicatePort = ItUtil.replicatePort(nodeId);
        int servicePort = ItUtil.servicePort(nodeId);
        // Create node directory
        File nodeDir = baseDir.resolve("node" + nodeId).toFile();
        if (!nodeDir.exists() && !nodeDir.mkdirs()) {
            throw new IOException("Failed to create node directory: " + nodeDir);
        }

        File dataDir = new File(nodeDir, "data");

        // Create config.properties
        File configFile = new File(nodeDir, "config.properties");
        Properties configProps = new Properties();
        configProps.setProperty("nodeId", String.valueOf(nodeId));
        configProps.setProperty("replicatePort", String.valueOf(replicatePort));
        configProps.setProperty("servicePort", String.valueOf(servicePort));
        configProps.setProperty("dataDir", dataDir.getAbsolutePath());

        // Add custom timeout settings if provided
        if (electTimeout != null) {
            configProps.setProperty("electTimeout", String.valueOf(electTimeout));
        }
        if (rpcTimeout != null) {
            configProps.setProperty("rpcTimeout", String.valueOf(rpcTimeout));
        }
        if (connectTimeout != null) {
            configProps.setProperty("connectTimeout", String.valueOf(connectTimeout));
        }
        if (heartbeatInterval != null) {
            configProps.setProperty("heartbeatInterval", String.valueOf(heartbeatInterval));
        }
        if (pingInterval != null) {
            configProps.setProperty("pingInterval", String.valueOf(pingInterval));
        }

        writeConfigFile(configProps, configFile);

        // Create servers.properties
        File serversFile = new File(nodeDir, "servers.properties");
        Properties serversProps = generateServersProperties(serversStr, groups);
        writeConfigFile(serversProps, serversFile);

        return new ProcessConfig(nodeDir, configFile, serversFile, nodeId, replicatePort, servicePort,
                electTimeout, rpcTimeout, connectTimeout, heartbeatInterval, pingInterval);
    }

    private static Properties generateServersProperties(String serversStr, List<GroupDefinition> groups) {
        Properties serversProps = new Properties();
        serversProps.setProperty("servers", serversStr);
        RaftGroupConfigEx protoType = new RaftGroupConfigEx(0, "", "");
        for (GroupDefinition group : groups) {
            serversProps.setProperty("group." + group.groupId + ".nodeIdOfMembers", group.nodeIdOfMembers);
            serversProps.setProperty("group." + group.groupId + ".nodeIdOfObservers", group.nodeIdOfObservers);
            serversProps.setProperty("group." + group.groupId + ".idxItemsPerFile", String.valueOf(protoType.idxItemsPerFile / 64));
            serversProps.setProperty("group." + group.groupId + ".idxCacheSize", String.valueOf(protoType.idxCacheSize / 64));
            serversProps.setProperty("group." + group.groupId + ".idxFlushThreshold", String.valueOf(protoType.idxFlushThreshold / 64));
            serversProps.setProperty("group." + group.groupId + ".logFileSize", String.valueOf(protoType.logFileSize / 64));
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

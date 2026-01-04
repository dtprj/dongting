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

        public ProcessConfig(File nodeDir, File configFile, File serversFile,
                             int nodeId, int replicatePort, int servicePort) {
            this.nodeDir = nodeDir;
            this.configFile = configFile;
            this.serversFile = serversFile;
            this.nodeId = nodeId;
            this.replicatePort = replicatePort;
            this.servicePort = servicePort;
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
            ProcessConfig files = generateNodeConfig(nid, baseDir, serversStr, groupDefinitions);
            result.add(files);
        }

        return result;
    }

    /**
     * Generate configuration files for a single node
     */
    public static ProcessConfig generateNodeConfig(int nodeId, Path baseDir, String serversStr,
                                                   List<GroupDefinition> groups) throws IOException {
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
        writeConfigFile(configProps, configFile);

        // Create servers.properties
        File serversFile = new File(nodeDir, "servers.properties");
        Properties serversProps = new Properties();
        serversProps.setProperty("servers", serversStr);
        for (GroupDefinition group : groups) {
            serversProps.setProperty("group." + group.groupId + ".nodeIdOfMembers", group.nodeIdOfMembers);
            serversProps.setProperty("group." + group.groupId + ".nodeIdOfObservers", group.nodeIdOfObservers);
        }
        writeConfigFile(serversProps, serversFile);

        return new ProcessConfig(nodeDir, configFile, serversFile, nodeId, replicatePort, servicePort);
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

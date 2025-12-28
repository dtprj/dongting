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
package com.github.dtprj.dongting.ops;

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.raft.QueryStatusResp;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.admin.AdminRaftClient;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class DtAdmin {

    private static final int ERR_COMMAND_LINE_ERROR = 101;
    private static final int ERR_LOAD_CONFIG_FAIL = 102;
    private static final int ERR_CLIENT_INIT_FAIL = 103;
    private static final int ERR_COMMAND_EXEC_FAIL = 104;

    private static final String GROUP_PREFIX = "group.";

    private static String serversFile;
    private static String subCommand;
    private static final Map<String, String> params = new HashMap<>();

    public static void main(String[] args) {
        try {
            parseArgs(args);
            Properties props = loadProperties(serversFile);
            AdminRaftClient client = initClient(props);
            try {
                executeCommand(client);
            } finally {
                client.stop(new DtTime(5, TimeUnit.SECONDS));
            }
        } catch (Throwable e) {
            System.err.println("Error: " + e.getMessage());
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            System.exit(getExitCode(e));
        }
    }

    private static void parseArgs(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(ERR_COMMAND_LINE_ERROR);
        }

        // parse -s option
        for (int i = 0; i < args.length; i++) {
            if ("-s".equals(args[i])) {
                if (i + 1 < args.length) {
                    serversFile = args[i + 1];
                    break;
                } else {
                    System.err.println("Error: -s option requires a file path");
                    System.exit(ERR_COMMAND_LINE_ERROR);
                }
            }
        }

        if (serversFile == null) {
            System.err.println("Error: -s option is required");
            printUsage();
            System.exit(ERR_COMMAND_LINE_ERROR);
        }

        // find subcommand
        boolean foundS = false;
        for (int i = 0; i < args.length; i++) {
            if ("-s".equals(args[i])) {
                foundS = true;
                i++; // skip file path
                continue;
            }
            if (foundS && !args[i].startsWith("--")) {
                subCommand = args[i];
                // parse remaining options
                for (int j = i + 1; j < args.length; j++) {
                    if (args[j].startsWith("--")) {
                        String key = args[j].substring(2);
                        if (j + 1 < args.length && !args[j + 1].startsWith("--")) {
                            params.put(key, args[j + 1]);
                            j++;
                        } else {
                            System.err.println("Error: option " + args[j] + " requires a value");
                            System.exit(ERR_COMMAND_LINE_ERROR);
                        }
                    }
                }
                break;
            }
        }

        if (subCommand == null) {
            System.err.println("Error: subcommand is required");
            printUsage();
            System.exit(ERR_COMMAND_LINE_ERROR);
        }
    }

    private static Properties loadProperties(String file) {
        try (FileInputStream fis = new FileInputStream(file)) {
            Properties props = new Properties();
            props.load(fis);
            return props;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config file " + file, e);
        }
    }

    private static AdminRaftClient initClient(Properties props) {
        try {
            String servers = props.getProperty("servers");
            if (servers == null || servers.trim().isEmpty()) {
                throw new RuntimeException("servers property is required");
            }

            AdminRaftClient client = new AdminRaftClient();
            client.start();
            client.clientAddNode(servers);

            // parse and add groups
            Set<Integer> groupIds = new HashSet<>();
            for (String key : props.stringPropertyNames()) {
                if (key.startsWith(GROUP_PREFIX)) {
                    String rest = key.substring(GROUP_PREFIX.length());
                    int dotIndex = rest.indexOf('.');
                    if (dotIndex > 0) {
                        String idStr = rest.substring(0, dotIndex);
                        try {
                            groupIds.add(Integer.parseInt(idStr));
                        } catch (NumberFormatException e) {
                            // ignore invalid group id
                        }
                    }
                }
            }

            for (int groupId : groupIds) {
                String members = props.getProperty(GROUP_PREFIX + groupId + ".nodeIdOfMembers");
                String observers = props.getProperty(GROUP_PREFIX + groupId + ".nodeIdOfObservers");
                if (members != null && !members.trim().isEmpty()) {
                    int[] memberIds = parseIntArray(members);
                    int[] observerIds = observers != null && !observers.trim().isEmpty() ?
                            parseIntArray(observers) : new int[0];
                    int[] allIds = new int[memberIds.length + observerIds.length];
                    System.arraycopy(memberIds, 0, allIds, 0, memberIds.length);
                    System.arraycopy(observerIds, 0, allIds, memberIds.length, observerIds.length);
                    client.clientAddOrUpdateGroup(groupId, allIds);
                }
            }

            return client;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize AdminRaftClient", e);
        }
    }

    private static void executeCommand(AdminRaftClient client) throws Exception {
        switch (subCommand) {
            case "transfer-leader":
                executeTransferLeader(client);
                break;
            case "prepare-config-change":
                executePrepareConfigChange(client);
                break;
            case "commit-change":
                executeCommitChange(client);
                break;
            case "abort-change":
                executeAbortChange(client);
                break;
            case "query-status":
                executeQueryStatus(client);
                break;
            case "add-group":
                executeServerAddGroup(client);
                break;
            case "remove-group":
                executeServerRemoveGroup(client);
                break;
            case "add-node":
                executeServerAddNode(client);
                break;
            case "remove-node":
                executeServerRemoveNode(client);
                break;
            case "list-nodes":
                executeServerListNodes(client);
                break;
            case "list-groups":
                executeServerListGroups(client);
                break;
            default:
                System.err.println("Error: Unknown subcommand: " + subCommand);
                printUsage();
                System.exit(ERR_COMMAND_LINE_ERROR);
        }
    }

    private static void executeTransferLeader(AdminRaftClient client) throws Exception {
        int groupId = getRequiredIntParam("group-id");
        int oldLeader = getRequiredIntParam("old-leader");
        int newLeader = getRequiredIntParam("new-leader");
        DtTime timeout = getTimeoutParam(30);

        System.out.println("Executing transfer-leader with timeout " + timeout.getTimeout(TimeUnit.SECONDS) + " seconds...");
        client.transferLeader(groupId, oldLeader, newLeader, timeout).get();
        System.out.println("Transfer leader completed successfully");
    }

    private static void executePrepareConfigChange(AdminRaftClient client) throws Exception {
        int groupId = getRequiredIntParam("group-id");
        Set<Integer> oldMembers = getRequiredIntSetParam("old-members");
        Set<Integer> oldObservers = getOptionalIntSetParam("old-observers");
        Set<Integer> newMembers = getRequiredIntSetParam("new-members");
        Set<Integer> newObservers = getOptionalIntSetParam("new-observers");
        DtTime timeout = getTimeoutParamOrDefault(client);

        System.out.println("Executing prepare-config-change with timeout " + timeout.getTimeout(TimeUnit.SECONDS) + " seconds...");
        long prepareIndex = client.prepareConfigChange(groupId, oldMembers, oldObservers,
                newMembers, newObservers, timeout).get();
        System.out.println("Prepare index: " + prepareIndex);
    }

    private static void executeCommitChange(AdminRaftClient client) throws Exception {
        int groupId = getRequiredIntParam("group-id");
        long prepareIndex = getRequiredLongParam("prepare-index");
        DtTime timeout = getTimeoutParamOrDefault(client);

        System.out.println("Executing commit-change with timeout " + timeout.getTimeout(TimeUnit.SECONDS) + " seconds...");
        long commitIndex = client.commitChange(groupId, prepareIndex, timeout).get();
        System.out.println("Commit index: " + commitIndex);
    }

    private static void executeAbortChange(AdminRaftClient client) throws Exception {
        int groupId = getRequiredIntParam("group-id");
        DtTime timeout = getTimeoutParamOrDefault(client);

        System.out.println("Executing abort-change with timeout " + timeout.getTimeout(TimeUnit.SECONDS) + " seconds...");
        long index = client.abortChange(groupId, timeout).get();
        System.out.println("Abort index: " + index);
    }

    private static void executeQueryStatus(AdminRaftClient client) throws Exception {
        int nodeId = getRequiredIntParam("node-id");
        int groupId = getRequiredIntParam("group-id");
        DtTime timeout = getTimeoutParamOrDefault(client);

        System.out.println("Executing query-status with timeout " + timeout.getTimeout(TimeUnit.SECONDS) + " seconds...");
        QueryStatusResp resp = client.queryRaftServerStatus(nodeId, groupId).get();
        System.out.println("Raft Status for group " + resp.groupId + " on node " + resp.nodeId + ":");
        System.out.println("  Term: " + resp.term);
        System.out.println("  Leader ID: " + resp.leaderId);
        System.out.println("  Commit Index: " + resp.commitIndex);
        System.out.println("  Last Applied: " + resp.lastApplied);
        System.out.println("  Last Log Index: " + resp.lastLogIndex);
        System.out.println("  Last Apply Time To Now (ms): " + resp.lastApplyTimeToNowMillis);
        System.out.println("  Apply Lag (ms): " + resp.applyLagMillis);
        System.out.println("  Init Finished: " + resp.isInitFinished());
        System.out.println("  Init Failed: " + resp.isInitFailed());
        System.out.println("  Group Ready: " + resp.isGroupReady());
        System.out.println("  Members: " + resp.members);
        System.out.println("  Observers: " + resp.observers);
        if (!resp.preparedMembers.isEmpty() || !resp.preparedObservers.isEmpty()) {
            System.out.println("  Prepared Members: " + resp.preparedMembers);
            System.out.println("  Prepared Observers: " + resp.preparedObservers);
        }
    }

    private static void executeServerAddGroup(AdminRaftClient client) throws Exception {
        int nodeId = getRequiredIntParam("node-id");
        int groupId = getRequiredIntParam("group-id");
        String members = getRequiredParam("members");
        String observers = getOptionalParam("observers", "");
        DtTime timeout = getTimeoutParam(30);

        System.out.println("Executing server-add-group with timeout " + timeout.getTimeout(TimeUnit.SECONDS) + " seconds...");
        client.serverAddGroup(nodeId, groupId, members, observers, timeout).get();
        System.out.println("Add group completed successfully");
    }

    private static void executeServerRemoveGroup(AdminRaftClient client) throws Exception {
        int nodeId = getRequiredIntParam("node-id");
        int groupId = getRequiredIntParam("group-id");
        DtTime timeout = getTimeoutParam(30);

        System.out.println("Executing server-remove-group with timeout " + timeout.getTimeout(TimeUnit.SECONDS) + " seconds...");
        client.serverRemoveGroup(nodeId, groupId, timeout).get();
        System.out.println("Remove group completed successfully");
    }

    private static void executeServerAddNode(AdminRaftClient client) throws Exception {
        int nodeId = getRequiredIntParam("node-id");
        int addNodeId = getRequiredIntParam("add-node-id");
        String host = getRequiredParam("host");
        int port = getRequiredIntParam("port");

        DtTime timeout = getTimeoutParamOrDefault(client);
        System.out.println("Executing server-add-node with timeout " + timeout.getTimeout(TimeUnit.SECONDS) + " seconds...");
        client.serverAddNode(nodeId, addNodeId, host, port).get();
        System.out.println("Add node completed successfully");
    }

    private static void executeServerRemoveNode(AdminRaftClient client) throws Exception {
        int nodeId = getRequiredIntParam("node-id");
        int removeNodeId = getRequiredIntParam("remove-node-id");

        DtTime timeout = getTimeoutParamOrDefault(client);
        System.out.println("Executing server-remove-node with timeout " + timeout.getTimeout(TimeUnit.SECONDS) + " seconds...");
        client.serverRemoveNode(nodeId, removeNodeId).get();
        System.out.println("Remove node completed successfully");
    }

    private static void executeServerListNodes(AdminRaftClient client) throws Exception {
        int nodeId = getRequiredIntParam("node-id");

        DtTime timeout = getTimeoutParamOrDefault(client);
        System.out.println("Executing server-list-nodes with timeout " + timeout.getTimeout(TimeUnit.SECONDS) + " seconds...");
        List<RaftNode> nodes = client.serverListNodes(nodeId).get();
        System.out.println("Nodes:");
        for (RaftNode node : nodes) {
            System.out.println("  Node{nodeId=" + node.nodeId + ", host=" + node.hostPort + "}");
        }
    }

    private static void executeServerListGroups(AdminRaftClient client) throws Exception {
        int nodeId = getRequiredIntParam("node-id");

        DtTime timeout = getTimeoutParamOrDefault(client);
        System.out.println("Executing server-list-groups with timeout " + timeout.getTimeout(TimeUnit.SECONDS) + " seconds...");
        int[] groupIds = client.serverListGroups(nodeId).get();
        System.out.println("Group IDs: " + Arrays.toString(groupIds));
    }

    private static String getRequiredParam(String name) {
        String value = params.get(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required parameter --" + name);
        }
        return value;
    }

    private static String getOptionalParam(String name, String defaultValue) {
        String value = params.get(name);
        return value != null && !value.trim().isEmpty() ? value : defaultValue;
    }

    private static int getRequiredIntParam(String name) {
        String value = getRequiredParam(name);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for --" + name + ": " + value);
        }
    }

    private static long getRequiredLongParam(String name) {
        String value = getRequiredParam(name);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long value for --" + name + ": " + value);
        }
    }

    private static Set<Integer> getRequiredIntSetParam(String name) {
        String value = getRequiredParam(name);
        return parseIntSet(value);
    }

    private static Set<Integer> getOptionalIntSetParam(String name) {
        String value = params.get(name);
        if (value == null || value.trim().isEmpty()) {
            return new HashSet<>();
        }
        return parseIntSet(value);
    }

    private static DtTime getTimeoutParam(int defaultSeconds) {
        String value = params.get("timeout");
        int seconds = defaultSeconds;
        if (value != null && !value.trim().isEmpty()) {
            try {
                seconds = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid timeout value: " + value);
            }
        }
        return new DtTime(seconds, TimeUnit.SECONDS);
    }

    private static DtTime getTimeoutParamOrDefault(AdminRaftClient client) {
        String value = params.get("timeout");
        if (value != null && !value.trim().isEmpty()) {
            try {
                int seconds = Integer.parseInt(value);
                return new DtTime(seconds, TimeUnit.SECONDS);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid timeout value: " + value);
            }
        }
        return client.createDefaultTimeout();
    }

    private static int[] parseIntArray(String value) {
        String[] parts = value.split(",");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Integer.parseInt(parts[i].trim());
        }
        return result;
    }

    private static Set<Integer> parseIntSet(String value) {
        String[] parts = value.split(",");
        Set<Integer> result = new HashSet<>();
        for (String part : parts) {
            result.add(Integer.parseInt(part.trim()));
        }
        return result;
    }

    private static int getExitCode(Throwable e) {
        if (e instanceof IllegalArgumentException) {
            return ERR_COMMAND_LINE_ERROR;
        }
        String msg = e.getMessage();
        if (msg != null && msg.contains("load config file")) {
            return ERR_LOAD_CONFIG_FAIL;
        }
        if (msg != null && msg.contains("initialize AdminRaftClient")) {
            return ERR_CLIENT_INIT_FAIL;
        }
        return ERR_COMMAND_EXEC_FAIL;
    }

    private static void printUsage() {
        System.out.println("Usage: dongting-admin.sh -s <servers.properties> <subcommand> [options]");
        System.out.println();
        System.out.println("Global Options:");
        System.out.println("  -s <file>          Path to servers.properties file (required)");
        System.out.println();
        System.out.println("Subcommands:");
        System.out.println();
        System.out.println("  list-nodes         List all node definitions on specified node");
        System.out.println("    --node-id <id>          Target node ID (required)");
        System.out.println();
        System.out.println("  list-groups        List all raft group IDs on specified node");
        System.out.println("    --node-id <id>          Target node ID (required)");
        System.out.println();
        System.out.println("  query-status       Query raft server status for specified group");
        System.out.println("    --node-id <id>          Target node ID (required)");
        System.out.println("    --group-id <id>         Raft group ID (required)");
        System.out.println("    --timeout <seconds>     Timeout in seconds (default: from createDefaultTimeout())");
        System.out.println();
        System.out.println("  transfer-leader    Transfer raft group leader");
        System.out.println("    --group-id <id>         Raft group ID (required)");
        System.out.println("    --old-leader <id>       Current leader node ID (required)");
        System.out.println("    --new-leader <id>       New leader node ID (required)");
        System.out.println("    --timeout <seconds>     Timeout in seconds (default: 30)");
        System.out.println();
        System.out.println("  prepare-config-change    Prepare configuration change");
        System.out.println("    --group-id <id>           Raft group ID (required)");
        System.out.println("    --old-members <ids>       Current member node IDs, comma-separated (required)");
        System.out.println("    --old-observers <ids>     Current observer node IDs, comma-separated (optional)");
        System.out.println("    --new-members <ids>       New member node IDs, comma-separated (required)");
        System.out.println("    --new-observers <ids>     New observer node IDs, comma-separated (optional)");
        System.out.println("    --timeout <seconds>       Timeout in seconds (default: from createDefaultTimeout())");
        System.out.println();
        System.out.println("  commit-change      Commit prepared configuration change");
        System.out.println("    --group-id <id>         Raft group ID (required)");
        System.out.println("    --prepare-index <idx>   Prepare index from prepare-config-change (required)");
        System.out.println("    --timeout <seconds>     Timeout in seconds (default: from createDefaultTimeout())");
        System.out.println();
        System.out.println("  abort-change       Abort prepared configuration change");
        System.out.println("    --group-id <id>         Raft group ID (required)");
        System.out.println("    --timeout <seconds>     Timeout in seconds (default: from createDefaultTimeout())");
        System.out.println();
        System.out.println("  add-group          Add and start a raft group on specified node");
        System.out.println("    --node-id <id>          Target node ID (required)");
        System.out.println("    --group-id <id>         Raft group ID (required)");
        System.out.println("    --members <ids>         Member node IDs, comma-separated (required)");
        System.out.println("    --observers <ids>       Observer node IDs, comma-separated (optional)");
        System.out.println("    --timeout <seconds>     Timeout in seconds (default: 30)");
        System.out.println();
        System.out.println("  remove-group       Remove and stop a raft group on specified node");
        System.out.println("    --node-id <id>          Target node ID (required)");
        System.out.println("    --group-id <id>         Raft group ID (required)");
        System.out.println("    --timeout <seconds>     Timeout in seconds (default: 30)");
        System.out.println();
        System.out.println("  add-node           Add node definition on specified node");
        System.out.println("    --node-id <id>          Node ID to invoke (required)");
        System.out.println("    --add-node-id <id>      Node ID to add (required)");
        System.out.println("    --host <host>           Host address (required)");
        System.out.println("    --port <port>           Port number (required)");
        System.out.println();
        System.out.println("  remove-node        Remove node definition on specified node");
        System.out.println("    --node-id <id>          Node ID to invoke (required)");
        System.out.println("    --remove-node-id <id>   Node ID to remove (required)");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  dongting-admin.sh -s conf/servers.properties transfer-leader --group-id 0 --old-leader 1 --new-leader 2");
        System.out.println("  dongting-admin.sh -s conf/servers.properties query-status --node-id 1 --group-id 0");
        System.out.println("  dongting-admin.sh -s conf/servers.properties list-groups --node-id 1");
    }
}

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

import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtkv.server.DtKV;
import com.github.dtprj.dongting.dtkv.server.KvServerConfig;
import com.github.dtprj.dongting.dtkv.server.KvServerUtil;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.DefaultRaftFactory;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author huangli
 */
public class Bootstrap {
    public static final int ERR_COMMAND_LINE_ERROR = 101;
    public static final int ERR_LOAD_CONFIG_FAIL = 102;
    public static final int ERR_BAD_DATA_DIR = 103;
    public static final int ERR_BOOTSTRAP_FAIL = 104;
    public static final int ERR_BAD_CONFIG = 105;
    public static final int ERR_START_FAIL = 106;

    public static final int DEFAULT_REPLICATE_PORT = 9331;
    public static final int DEFAULT_SERVICE_PORT = 9332;

    public static final String GROUP_PREFIX = "group.";

    private int exitCode;

    private Properties serversProps;
    private RaftServerConfig serverConfig;
    private List<RaftGroupConfig> groupConfigs;
    private File serversFile;

    private volatile ExecutorService ioExecutor;
    private volatile RaftServer raftServer;

    public static void main(String[] args) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.run(args);
        if (bootstrap.exitCode != 0) {
            System.exit(bootstrap.exitCode);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                bootstrap.shutdown(new DtTime(60, TimeUnit.SECONDS))));
    }

    private void updateExitCode(int code) {
        if (exitCode == 0) {
            exitCode = code;
        }
    }

    public void run(String[] args) {
        try {
            String configFile = parseConfigFileFromCommandArgs(args, "-c");
            String serversConfigFile = parseConfigFileFromCommandArgs(args, "-s");

            Properties configProps = loadProperties(configFile);
            System.out.println("Config file loaded successfully: " + configFile);

            this.serversProps = loadProperties(serversConfigFile);
            System.out.println("Servers/groups config file loaded successfully: " + serversConfigFile);

            this.serversFile = new File(serversConfigFile);

            this.serverConfig = new RaftServerConfig();
            PropsUtil.setFieldsFromProps(serverConfig, configProps, "");

            System.out.println("LOG_DIR=" + System.getProperty("LOG_DIR"));

            serverConfig.servers = serversProps.getProperty("servers");
            if (serverConfig.servers == null) {
                System.err.println("servers property is required.");
                updateExitCode(ERR_BAD_CONFIG);
                return;
            }
            if (serverConfig.replicatePort == 0) {
                serverConfig.replicatePort = DEFAULT_REPLICATE_PORT;
            }
            if (serverConfig.servicePort == 0) {
                serverConfig.servicePort = DEFAULT_SERVICE_PORT;
            }

            boolean logDataDir = false;
            this.groupConfigs = new ArrayList<>();
            for (int groupId : parseGroupIds(serversProps)) {
                String nodeIdOfMembers = serversProps.getProperty(GROUP_PREFIX + groupId + ".nodeIdOfMembers");
                String nodeIdOfObservers = serversProps.getProperty(GROUP_PREFIX + groupId + ".nodeIdOfObservers");
                RaftGroupConfig groupConfig = RaftGroupConfig.newInstance(groupId, nodeIdOfMembers, nodeIdOfObservers);
                PropsUtil.setFieldsFromProps(groupConfig, serversProps, GROUP_PREFIX + groupId);
                groupConfigs.add(groupConfig);
                if (!logDataDir) {
                    System.out.println("DATA_DIR=" + groupConfig.dataDir);
                    logDataDir = true;
                }
                groupConfig.dataDir = groupConfig.dataDir + "/" + groupId;
                String s = ensureDir(groupConfig.dataDir);
                if (s != null) {
                    System.err.println(s);
                    updateExitCode(ERR_BAD_DATA_DIR);
                    return;
                }
            }
            if (groupConfigs.isEmpty()) {
                System.err.println("No group configs found.");
                updateExitCode(ERR_BAD_CONFIG);
                return;
            }

        } catch (Throwable e) {
            System.err.println("Failed to start server: " + e);
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            updateExitCode(ERR_BOOTSTRAP_FAIL);
            return;
        }

        startRaftServer();
    }

    private void startRaftServer() {
        try {
            AtomicInteger count = new AtomicInteger();
            ioExecutor = Executors.newFixedThreadPool(serverConfig.blockIoThreads,
                    r -> new Thread(r, "raft-io-" + count.incrementAndGet()));
            raftServer = new RaftServer(serverConfig, groupConfigs, createRaftFactory());
            KvServerUtil.initKvServer(raftServer);
            SyncConfigProcessor p = new SyncConfigProcessor(raftServer, serversFile);
            raftServer.getNioServer().register(Commands.RAFT_ADMIN_SYNC_CONFIG, p, ioExecutor);
            raftServer.start();
        } catch (Throwable e) {
            System.err.println("Failed to start server: " + e);
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            updateExitCode(ERR_START_FAIL);
        }
    }

    private DefaultRaftFactory createRaftFactory() {
        return new DefaultRaftFactory() {
            @Override
            public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
                KvServerConfig kvConfig = new KvServerConfig();
                PropsUtil.setFieldsFromProps(kvConfig, serversProps, GROUP_PREFIX + groupConfig.groupId);
                return new DtKV(groupConfig, kvConfig);
            }

            // this method called when add group at runtime
            @Override
            public RaftGroupConfig createConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
                RaftGroupConfig groupConfig = RaftGroupConfig.newInstance(groupId, nodeIdOfMembers, nodeIdOfObservers);
                PropsUtil.setFieldsFromProps(groupConfig, serversProps, GROUP_PREFIX + groupId);
                groupConfig.dataDir = groupConfig.dataDir + "/" + groupId;
                String s = ensureDir(groupConfig.dataDir);
                if (s != null) {
                    throw new RaftException(s);
                }
                return groupConfig;
            }

            @Override
            public ExecutorService createBlockIoExecutor(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig) {
                return ioExecutor;
            }

            @Override
            public void shutdownBlockIoExecutor(RaftServerConfig serverConfig, RaftGroupConfigEx groupConfig,
                                                ExecutorService executor) {
            }
        };
    }

    private String parseConfigFileFromCommandArgs(String[] args, String option) {
        for (int i = 0; i < args.length; i++) {
            if (option.equals(args[i])) {
                if (i + 1 < args.length) {
                    return args[i + 1];
                } else {
                    updateExitCode(ERR_COMMAND_LINE_ERROR);
                    throw new IllegalArgumentException(option + " option requires a config file path");
                }
            }
        }
        updateExitCode(ERR_COMMAND_LINE_ERROR);
        throw new IllegalArgumentException("config file not specified. Use " + option + " <config-file>");
    }

    private Properties loadProperties(String configFile) {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            props.load(fis);
            return props;
        } catch (Exception e) {
            updateExitCode(ERR_LOAD_CONFIG_FAIL);
            throw new RuntimeException("Failed to load config file " + configFile, e);
        }
    }

    private String ensureDir(String dir) {
        try {
            File dirFile = new File(dir);
            if (!dirFile.exists()) {
                if (!dirFile.mkdirs()) {
                    return "Error: failed to create directory " + dir;
                }
            }
            if (!dirFile.isDirectory()) {
                return "Error: " + dir + " is not a directory";
            }

            File checkFile = new File(dirFile, "check");
            if (!checkFile.exists()) {
                if (!checkFile.createNewFile()) {
                    return "Error: dir " + dir + " write check fail";
                }
            }
            if (!checkFile.delete()) {
                return "Error: dir " + dir + " write check fail";
            }
            return null;
        } catch (Exception e) {
            return "Error: dir" + dir + " check fail : " + e;
        }
    }

    private int[] parseGroupIds(Properties props) {
        HashSet<Integer> groupIds = new HashSet<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(GROUP_PREFIX)) {
                String rest = key.substring(GROUP_PREFIX.length());
                int dotIndex = rest.indexOf('.');
                if (dotIndex <= 0) {
                    updateExitCode(ERR_BAD_CONFIG);
                    throw new IllegalArgumentException("Invalid group config key: " + key);
                }
                String idStr = rest.substring(0, dotIndex);
                try {
                    groupIds.add(Integer.parseInt(idStr));
                } catch (NumberFormatException e) {
                    updateExitCode(ERR_BAD_CONFIG);
                    throw new IllegalArgumentException("Invalid group id in key: " + key
                            + ", group id must be a number", e);
                }
            }
        }
        for (int groupId : groupIds) {
            String nodeIdOfMembers = props.getProperty(GROUP_PREFIX + groupId + ".nodeIdOfMembers");
            if (nodeIdOfMembers == null || nodeIdOfMembers.isEmpty()) {
                updateExitCode(ERR_BAD_CONFIG);
                throw new IllegalArgumentException("Missing nodeIdOfMembers for group " + groupId);
            }
        }
        return groupIds.stream().sorted().mapToInt(Integer::intValue).toArray();
    }

    public void shutdown(DtTime timeout) {
        if (raftServer != null) {
            raftServer.stop(timeout);
            raftServer = null;
        }
        if (ioExecutor != null) {
            ioExecutor.shutdown();
            ioExecutor = null;
        }
    }
}

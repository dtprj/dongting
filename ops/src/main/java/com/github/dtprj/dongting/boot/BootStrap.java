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
package com.github.dtprj.dongting.boot;

import com.github.dtprj.dongting.dtkv.server.DtKV;
import com.github.dtprj.dongting.dtkv.server.KvServerConfig;
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
import java.util.List;
import java.util.Properties;

/**
 * @author huangli
 */
public class BootStrap {

    public static final int ERR_COMMAND_LINE_ERROR = 101;
    public static final int ERR_LOAD_CONFIG_FAIL = 102;
    public static final int ERR_BAD_LOG_DIR = 103;
    public static final int ERR_BAD_DATA_DIR = 104;
    public static final int ERR_BOOTSTRAP_FAIL = 105;
    public static final int ERR_BAD_CONFIG = 106;
    public static final int ERR_START_FAIL = 107;

    public static final int DEFAULT_REPLICATE_PORT = 9331;
    public static final int DEFAULT_SERVICE_PORT = 9332;

    public static void main(String[] args) {
        Properties configProps = null;
        RaftServerConfig serverConfig = null;
        List<RaftGroupConfig> groupConfigs = null;
        try {
            String configFile = parseConfigFileFromCommandArgs(args, "-c");
            String serversConfigFile = parseConfigFileFromCommandArgs(args, "-s");

            configProps = loadProperties(configFile);
            System.out.println("Config file loaded successfully: " + configFile);

            Properties serversProps = loadProperties(serversConfigFile);
            System.out.println("Servers/groups config file loaded successfully: " + serversConfigFile);

            String logDir = configProps.getProperty("LOG_DIR", "logs");
            String s = ensureDir(logDir);
            if (s != null) {
                System.err.println(s);
                System.exit(ERR_BAD_LOG_DIR);
            }
            System.setProperty("LOG_DIR", logDir);
            System.out.println("use log dir: " + logDir);

            serverConfig = new RaftServerConfig();
            PropsUtil.setFieldsFromProps(serverConfig, configProps, "");

            serverConfig.servers = serversProps.getProperty("servers");
            if (serverConfig.servers == null) {
                System.err.println("servers property is required.");
                System.exit(ERR_BAD_CONFIG);
            }
            if (serverConfig.replicatePort == 0) {
                serverConfig.replicatePort = DEFAULT_REPLICATE_PORT;
            }
            if (serverConfig.servicePort == 0) {
                serverConfig.servicePort = DEFAULT_SERVICE_PORT;
            }

            groupConfigs = new ArrayList<>();
            for (int groupId = 0; ; groupId++) {
                String nodeIdOfMembers = serversProps.getProperty("group." + groupId + ".nodeIdOfMembers");
                if (nodeIdOfMembers == null) {
                    break;
                }
                String nodeIdOfObservers = serversProps.getProperty("group." + groupId + ".nodeIdOfObservers");
                RaftGroupConfig groupConfig = RaftGroupConfig.newInstance(groupId, nodeIdOfMembers, nodeIdOfObservers);
                PropsUtil.setFieldsFromProps(groupConfig, configProps, "");
                groupConfigs.add(groupConfig);
                groupConfig.dataDir = groupConfig.dataDir + "/" + groupId;
                s = BootStrap.ensureDir(groupConfig.dataDir);
                if (s != null) {
                    System.err.println(s);
                    System.exit(ERR_BAD_DATA_DIR);
                }
            }
            if (groupConfigs.isEmpty()) {
                System.err.println("No group configs found.");
                System.exit(BootStrap.ERR_BAD_CONFIG);
            }

        } catch (Throwable e) {
            System.err.println("Failed to start server");
            e.printStackTrace();
            System.exit(ERR_BOOTSTRAP_FAIL);
        }

        start(serverConfig, groupConfigs, configProps);
    }

    private static void start(RaftServerConfig serverConfig, List<RaftGroupConfig> groupConfigs, Properties configProps) {
        RaftServer raftServer = new RaftServer(serverConfig, groupConfigs, new DefaultRaftFactory() {
            @Override
            public StateMachine createStateMachine(RaftGroupConfigEx groupConfig) {
                return new DtKV(groupConfig, new KvServerConfig());
            }

            // this method called when add group at runtime
            @Override
            public RaftGroupConfig createConfig(int groupId, String nodeIdOfMembers, String nodeIdOfObservers) {
                RaftGroupConfig groupConfig = RaftGroupConfig.newInstance(groupId, nodeIdOfMembers, nodeIdOfObservers);
                PropsUtil.setFieldsFromProps(groupConfig, configProps, "");
                groupConfig.dataDir = groupConfig.dataDir + "/" + groupId;
                String s = BootStrap.ensureDir(groupConfig.dataDir);
                if (s != null) {
                    throw new RaftException(s);
                }
                return groupConfig;
            }
        });
        try {
            raftServer.start();
        } catch (Throwable e) {
            System.err.println("Failed to start server");
            e.printStackTrace();
            System.exit(ERR_START_FAIL);
        }
    }

    private static String parseConfigFileFromCommandArgs(String[] args, String option) {
        for (int i = 0; i < args.length; i++) {
            if (option.equals(args[i])) {
                if (i + 1 < args.length) {
                    return args[i + 1];
                } else {
                    System.err.println("Error: -c option requires a config file path");
                    System.exit(ERR_COMMAND_LINE_ERROR);
                    return null;
                }
            }
        }
        System.err.println("Error: config file not specified. Use -c <config-file>");
        System.exit(ERR_COMMAND_LINE_ERROR);
        return null;
    }

    private static Properties loadProperties(String configFile) {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            props.load(fis);
            return props;
        } catch (Exception e) {
            System.err.println("Error: failed to load config file " + configFile + ": " + e);
            System.exit(ERR_LOAD_CONFIG_FAIL);
            return null;
        }
    }

    static String ensureDir(String dir) {
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
}

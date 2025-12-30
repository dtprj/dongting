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

import com.github.dtprj.dongting.dtkv.server.DtKV;
import com.github.dtprj.dongting.dtkv.server.KvServerConfig;
import com.github.dtprj.dongting.dtkv.server.KvServerUtil;
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

    private static final String GROUP_PREFIX = "group.";

    public static void main(String[] args) {
        new Bootstrap().run(args);
    }

    public void run(String[] args) {
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

            serverConfig = new RaftServerConfig();
            PropsUtil.setFieldsFromProps(serverConfig, configProps, "");

            System.out.println("LOG_DIR=" + System.getProperty("LOG_DIR"));

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

            boolean logDataDir = false;
            groupConfigs = new ArrayList<>();
            for (int groupId : parseGroupIds(serversProps)) {
                String nodeIdOfMembers = serversProps.getProperty(GROUP_PREFIX + groupId + ".nodeIdOfMembers");
                String nodeIdOfObservers = serversProps.getProperty(GROUP_PREFIX + groupId + ".nodeIdOfObservers");
                RaftGroupConfig groupConfig = RaftGroupConfig.newInstance(groupId, nodeIdOfMembers, nodeIdOfObservers);
                PropsUtil.setFieldsFromProps(groupConfig, configProps, "");
                groupConfigs.add(groupConfig);
                if (!logDataDir) {
                    System.out.println("DATA_DIR=" + groupConfig.dataDir);
                    logDataDir = true;
                }
                groupConfig.dataDir = groupConfig.dataDir + "/" + groupId;
                String s = ensureDir(groupConfig.dataDir);
                if (s != null) {
                    System.err.println(s);
                    System.exit(ERR_BAD_DATA_DIR);
                }
            }
            if (groupConfigs.isEmpty()) {
                System.err.println("No group configs found.");
                System.exit(Bootstrap.ERR_BAD_CONFIG);
            }

        } catch (Throwable e) {
            System.err.println("Failed to start server: " + e);
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            System.exit(ERR_BOOTSTRAP_FAIL);
        }

        start(serverConfig, groupConfigs, configProps);
    }


    private void start(RaftServerConfig serverConfig, List<RaftGroupConfig> groupConfigs, Properties configProps) {
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
                String s = ensureDir(groupConfig.dataDir);
                if (s != null) {
                    throw new RaftException(s);
                }
                return groupConfig;
            }
        });
        try {
            KvServerUtil.initKvServer(raftServer);
            raftServer.start();
        } catch (Throwable e) {
            System.err.println("Failed to start server: " + e);
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
            System.exit(ERR_START_FAIL);
        }
    }

    private String parseConfigFileFromCommandArgs(String[] args, String option) {
        for (int i = 0; i < args.length; i++) {
            if (option.equals(args[i])) {
                if (i + 1 < args.length) {
                    return args[i + 1];
                } else {
                    System.err.println("Error: " + option + " option requires a config file path");
                    System.exit(ERR_COMMAND_LINE_ERROR);
                    return null;
                }
            }
        }
        System.err.println("Error: config file not specified. Use " + option + " <config-file>");
        System.exit(ERR_COMMAND_LINE_ERROR);
        return null;
    }

    private Properties loadProperties(String configFile) {
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
                    System.err.println("Invalid group config key: " + key);
                    System.exit(ERR_BAD_CONFIG);
                }
                String idStr = rest.substring(0, dotIndex);
                try {
                    groupIds.add(Integer.parseInt(idStr));
                } catch (NumberFormatException e) {
                    System.err.println("Invalid group id in key: " + key + ", group id must be a number");
                    System.exit(ERR_BAD_CONFIG);
                }
            }
        }
        for (int groupId : groupIds) {
            String nodeIdOfMembers = props.getProperty(GROUP_PREFIX + groupId + ".nodeIdOfMembers");
            if (nodeIdOfMembers == null || nodeIdOfMembers.isEmpty()) {
                System.err.println("Missing nodeIdOfMembers for group " + groupId);
                System.exit(ERR_BAD_CONFIG);
            }
        }
        return groupIds.stream().sorted().mapToInt(Integer::intValue).toArray();
    }
}

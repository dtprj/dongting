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
package com.github.dtprj.dongting.raft;

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.ObjUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioClient;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.net.NioServerConfig;
import com.github.dtprj.dongting.net.Peer;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author huangli
 */
public class RaftServer extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(RaftServer.class);
    private final RaftServerConfig config;
    private final NioServer server;
    private final NioClient client;
    private final Set<HostPort> servers;
    private final int electQuorum;
    private final int rwQuorum;
    private final GroupConManager groupConManager;

    public RaftServer(RaftServerConfig config) {
        this.config = config;
        Objects.requireNonNull(config.getServers());
        ObjUtil.checkPositive(config.getId(), "id");
        ObjUtil.checkPositive(config.getPort(), "port");

        servers = parseServers(config.getServers());

        electQuorum = servers.size() / 2 + 1;
        rwQuorum = servers.size() % 2 == 0 ? servers.size() / 2 : electQuorum;

        NioServerConfig nioServerConfig = new NioServerConfig();
        nioServerConfig.setPort(config.getPort());
        nioServerConfig.setName("RaftServer");
        server = new NioServer(nioServerConfig);

        NioClientConfig nioClientConfig = new NioClientConfig();
        nioClientConfig.setName("RaftClient");
        client = new NioClient(nioClientConfig);

        groupConManager = new GroupConManager(config.getId(), servers, config.getServers(), client);
        server.register(Commands.RAFT_HANDSHAKE, this.groupConManager.getProcessor());
    }

    static Set<HostPort> parseServers(String serversStr) {
        Set<HostPort> servers = Arrays.stream(serversStr.split("[,;]"))
                .filter(Objects::nonNull)
                .map(s -> {
                    String[] arr = s.split(":");
                    if (arr.length != 2) {
                        throw new IllegalArgumentException("not 'host:port' format:" + s);
                    }
                    return new HostPort(arr[0].trim(), Integer.parseInt(arr[1].trim()));
                }).collect(Collectors.toSet());
        if (servers.size() == 0) {
            throw new DtException("servers list is empty");
        }
        return servers;
    }

    @Override
    protected void doStart() {
        server.start();
        client.start();
        client.waitStart();
        while (true) {
            CompletableFuture<List<Peer>> f = groupConManager.fetch();
            try {
                List<Peer> peers = f.get(15, TimeUnit.SECONDS);
                int currentNodes = peers.size() + 1;
                if (currentNodes >= electQuorum) {
                    log.info("raft group init success. electQuorum={}, currentNodes={}. remote peers: {}",
                            electQuorum, currentNodes, peers);
                    break;
                }
            } catch (InterruptedException e) {
                throw new RaftException(e);
            } catch (ExecutionException e) {
                throw new RaftException(e);
            } catch (TimeoutException e) {
                log.warn("init raft group timeout, will continue");
            }
        }
    }

    @Override
    protected void doStop() {
        server.stop();
        client.stop();
    }

}

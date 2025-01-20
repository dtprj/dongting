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

import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioNet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * @author huangli
 */
public class RaftNode {
    private final int nodeId;
    private final HostPort hostPort;

    private String str;

    public RaftNode(int nodeId, HostPort hostPort) {
        this.nodeId = nodeId;
        this.hostPort = hostPort;
    }

    public int getNodeId() {
        return nodeId;
    }

    public HostPort getHostPort() {
        return hostPort;
    }

    @Override
    public String toString() {
        if (str == null) {
            str = nodeId + "@" + hostPort;
        }
        return str;
    }

    public static List<RaftNode> parseServers(String serversStr) {
        if (serversStr == null || serversStr.isEmpty()) {
            //noinspection unchecked
            return Collections.EMPTY_LIST;
        }
        String[] servers = serversStr.split(";");
        if (servers.length == 0) {
            throw new RaftException("servers list is empty");
        }
        try {
            List<RaftNode> list = new ArrayList<>();
            for (String server : servers) {
                String[] arr = server.split(",");
                if (arr.length != 2) {
                    throw new IllegalArgumentException("not 'id,host:port' format:" + server);
                }
                int id = Integer.parseInt(arr[0].trim());
                String hostPortStr = arr[1];
                list.add(new RaftNode(id, NioNet.parseHostPort(hostPortStr)));
            }
            return list;
        } catch (NumberFormatException e) {
            throw new RaftException("bad servers list: " + serversStr);
        }
    }

    public static String formatServers(List<RaftNode> servers) {
        return formatServers(servers, Function.identity());
    }

    public static <T> String formatServers(List<T> servers, Function<T, ? extends RaftNode> mapper) {
        StringBuilder sb = new StringBuilder();
        for (T s : servers) {
            RaftNode n = mapper.apply(s);
            HostPort hp = n.getHostPort();
            sb.append(n.getNodeId()).append(",").append(hp.getHost()).append(":").append(hp.getPort()).append(";");
        }
        return sb.toString();
    }
}

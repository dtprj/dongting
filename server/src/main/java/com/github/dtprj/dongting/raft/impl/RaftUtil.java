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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.raft.client.RaftException;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author huangli
 */
public class RaftUtil {
    public static Set<HostPort> parseServers(String serversStr) {
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
            throw new RaftException("servers list is empty");
        }
        return servers;
    }
}

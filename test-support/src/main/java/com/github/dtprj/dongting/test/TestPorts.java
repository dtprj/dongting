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
package com.github.dtprj.dongting.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Picks a temp port for tests, so test processes of parallel worktrees do not conflict
 * on hard-coded ports.
 *
 * @author huangli
 */
public final class TestPorts {

    private static final Set<Integer> USED_PORTS = ConcurrentHashMap.newKeySet();
    private static final Map<Integer, Integer> SHARED_PORTS = new ConcurrentHashMap<>();

    /**
     * Ports with the same index are shared by all tests in this jvm, like the old hard-coded ports.
     */
    public static int getPort() {
        return getPort(0);
    }

    public static int getPort(int index) {
        return SHARED_PORTS.computeIfAbsent(index, k -> nextPort());
    }

    private static int nextPort() {
        while (true) {
            // below the default os ephemeral range (linux 32768+, macos/windows 49152+): an outbound
            // source port picked from that range would occasionally occupy an allocated-but-not-yet-bound port
            int port = 10000 + ThreadLocalRandom.current().nextInt(22000);
            if (!USED_PORTS.add(port)) {
                continue;
            }
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(false);
                socket.bind(new InetSocketAddress(port));
                return port;
            } catch (BindException e) {
                // taken by another process, give the candidate back and try another
                USED_PORTS.remove(port);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private TestPorts() {
    }
}

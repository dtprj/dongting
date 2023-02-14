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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.common.CloseUtil;
import com.github.dtprj.dongting.common.DtTime;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
public class ConnectTimeoutTest {

    @Test
    public void connectTimeoutTest() throws Exception {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.setPort(8888);
        NioServer server = new NioServer(serverConfig);
        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.setCleanInterval(0);
        clientConfig.setSelectTimeout(1);
        clientConfig.setMaxOutRequests(1);
        NioClient client = new NioClient(clientConfig);

        try {
            server.start();
            client.start();
            client.waitStart();

            Peer peer = client.addPeer(new HostPort("127.0.0.1", 8888)).get(1, TimeUnit.SECONDS);

            DtTime deadline = new DtTime(System.nanoTime() - 1000000000L, 1, TimeUnit.NANOSECONDS);
            try {
                client.connect(peer, deadline).get(1, TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof NetTimeoutException);
            }

            deadline = new DtTime(1, TimeUnit.SECONDS);
            client.connect(peer, deadline).get(1, TimeUnit.SECONDS);
        } finally {
            CloseUtil.close(server, client);
        }

    }
}

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
package com.github.dtprj.dongting.remoting;

import com.github.dtprj.dongting.common.DtTime;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class NioServerTest {
    private static final int LOOP = 100;
    private static final int SIZE = 5;

    public static void main(String[] args) throws Exception {
        NioServerConfig c = new NioServerConfig();
        c.setIoThreads(1);
        c.setPort(9000);
        NioServer server = new NioServer(c);

        server.start();

        Thread.sleep(100);
        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        NioClient client = new NioClient(clientConfig);
        client.start();

        for (int i = 0; i < LOOP; i++) {
            ByteBufferWriteFrame req = new ByteBufferWriteFrame();
            req.setFrameType(CmdType.TYPE_REQ);
            req.setCommand(Commands.CMD_PING);
            req.setBody(ByteBuffer.wrap(new byte[SIZE]));
            CompletableFuture<ReadFrame> future = client.sendRequest(req,
                    ByteBufferDecoder.INSTANCE, new DtTime(1, TimeUnit.SECONDS));

            ByteBuffer buf = (ByteBuffer) future.get().getBody();
            System.out.println("client get " + buf.remaining());
        }

        client.stop();

        server.stop();
    }
}

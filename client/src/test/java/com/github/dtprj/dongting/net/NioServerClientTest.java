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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
public class NioServerClientTest {

    @Test
    public void simpleTest() throws Exception {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.setPort(9000);
        NioServer server = new NioServer(serverConfig);
        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        NioClient client = new NioClient(clientConfig);
        try {
            server.start();
            client.start();
            client.waitStart();
            invoke(client);
        } finally {
            CloseUtil.close(client, server);
        }
    }

    private static void invoke(NioClient client) throws Exception {
        ByteBufferWriteFrame wf = new ByteBufferWriteFrame();
        ByteBuffer buf = ByteBuffer.allocate(3000);
        new Random().nextBytes(buf.array());
        wf.setCommand(Commands.CMD_PING);
        wf.setFrameType(FrameType.TYPE_REQ);
        wf.setBody(buf);
        CompletableFuture<ReadFrame> f = client.sendRequest(wf, ByteBufferDecoder.INSTANCE, new DtTime(1, TimeUnit.SECONDS));
        ReadFrame rf = f.get(1, TimeUnit.SECONDS);
        assertEquals(wf.getSeq(), rf.getSeq());
        assertEquals(FrameType.TYPE_RESP, rf.getFrameType());
        assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
        assertArrayEquals(buf.array(), ((ByteBuffer) rf.getBody()).array());
    }

    @Test
    public void testSeqProblem() throws Exception {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.setPort(9000);
        NioServer server = new NioServer(serverConfig);
        server.register(12345, new NioServer.PingProcessor() {
            @Override
            public WriteFrame process(ReadFrame frame, DtChannel channel) {
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return super.process(frame, channel);
            }
        });

        NioClientConfig clientConfig = new NioClientConfig();

        clientConfig.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        NioClient client = new NioClient(clientConfig);
        try {
            server.start();
            client.start();
            client.waitStart();

            DtChannel dtc = client.getPeers().get(0).getDtChannel();


            // seq int32 overflow test
            dtc.seq = Integer.MAX_VALUE - 1;
            for (int i = 0; i < 5; i++) {
                invoke(client);
            }

            // dup seq test
            ByteBufferWriteFrame wf1 = new ByteBufferWriteFrame();
            wf1.setCommand(12345);
            wf1.setFrameType(FrameType.TYPE_REQ);
            wf1.setBody(ByteBufferPool.EMPTY_BUFFER);

            ByteBufferWriteFrame wf2 = new ByteBufferWriteFrame();
            wf2.setCommand(12345);
            wf2.setFrameType(FrameType.TYPE_REQ);
            wf2.setBody(ByteBufferPool.EMPTY_BUFFER);

            CompletableFuture<ReadFrame> f1 = client.sendRequest(wf1, ByteBufferDecoder.INSTANCE, new DtTime(1, TimeUnit.SECONDS));
            Thread.sleep(10);// wait dispatch thread
            dtc.seq = dtc.seq - 1;
            CompletableFuture<ReadFrame> f2 = client.sendRequest(wf2, ByteBufferDecoder.INSTANCE, new DtTime(1, TimeUnit.SECONDS));
            ReadFrame rf1 = f1.get(1, TimeUnit.SECONDS);
            Assertions.assertEquals(CmdCodes.SUCCESS, rf1.getRespCode());

            try {
                f2.get(1, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                assertEquals(NetException.class, e.getCause().getClass());
            }

        } finally {
            CloseUtil.close(client, server);
        }
    }

    @Test
    public void timeoutTest() throws Exception {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.setPort(9000);
        NioServer server = new NioServer(serverConfig);
        NioClientConfig clientConfig = new NioClientConfig();
        clientConfig.setCleanIntervalMills(1);
        clientConfig.setSelectTimeoutMillis(1);
        clientConfig.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        server.register(2000, new NioServer.PingProcessor(){
            @Override
            public WriteFrame process(ReadFrame frame, DtChannel channel) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return super.process(frame, channel);
            }
        });
        NioClient client = new NioClient(clientConfig);
        try {
            server.start();
            client.start();
            client.waitStart();

            ByteBufferWriteFrame wf = new ByteBufferWriteFrame();
            wf.setCommand(2000);
            wf.setFrameType(FrameType.TYPE_REQ);
            wf.setBody(ByteBufferPool.EMPTY_BUFFER);
            CompletableFuture<ReadFrame> f = client.sendRequest(wf,
                    ByteBufferDecoder.INSTANCE, new DtTime(1, TimeUnit.MILLISECONDS));
            f.get(1, TimeUnit.SECONDS);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            //ensure connection status is correct after timeout
            invoke(client);
        } finally {
            CloseUtil.close(client, server);
        }
    }
}

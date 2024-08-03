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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.SimpleByteBufferPool;
import com.github.dtprj.dongting.codec.RefBufferDecoder;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            TestUtil.stop(client, server);
        }
    }

    static void invoke(NioClient client) throws Exception {
        Random r = new Random();
        int len = (r.nextInt(10) == 0) ? 0 : r.nextInt(3000);
        ByteBuffer buf = ByteBuffer.allocate(len);
        r.nextBytes(buf.array());
        ByteBufferWriteFrame wf = new ByteBufferWriteFrame(buf);
        wf.setCommand(Commands.CMD_PING);

        CompletableFuture<ReadFrame<RefBuffer>> f = client.sendRequest(wf, RefBufferDecoder.INSTANCE, new DtTime(1, TimeUnit.SECONDS));
        ReadFrame<RefBuffer> rf = f.get(1, TimeUnit.SECONDS);
        assertEquals(wf.getSeq(), rf.getSeq());
        assertEquals(FrameType.TYPE_RESP, rf.getFrameType());
        assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
        RefBuffer rc = rf.getBody();
        if (rc != null) {
            assertEquals(buf, rc.getBuffer());
            rc.release();
        }
    }

    @Test
    public void testSeqProblem() throws Exception {
        NioServerConfig serverConfig = new NioServerConfig();
        serverConfig.setPort(9000);
        NioServer server = new NioServer(serverConfig);
        server.register(12345, new NioServer.PingProcessor() {
            @Override
            public WriteFrame process(ReadFrame<RefBuffer> frame, ReqContext reqContext) {
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return super.process(frame, reqContext);
            }
        });

        NioClientConfig clientConfig = new NioClientConfig();

        clientConfig.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        NioClient client = new NioClient(clientConfig);
        try {
            server.start();
            client.start();
            client.waitStart();

            DtChannelImpl dtc = client.getPeers().get(0).getDtChannel();


            // seq int32 overflow test
            dtc.seq = Integer.MAX_VALUE - 1;
            for (int i = 0; i < 5; i++) {
                invoke(client);
            }

            // dup seq test
            ByteBufferWriteFrame wf1 = new ByteBufferWriteFrame(SimpleByteBufferPool.EMPTY_BUFFER);
            wf1.setCommand(12345);

            ByteBufferWriteFrame wf2 = new ByteBufferWriteFrame(SimpleByteBufferPool.EMPTY_BUFFER);
            wf2.setCommand(12345);

            CompletableFuture<ReadFrame<RefBuffer>> f1 = client.sendRequest(wf1, RefBufferDecoder.INSTANCE, new DtTime(1, TimeUnit.SECONDS));
            Thread.sleep(10);// wait dispatch thread
            dtc.seq = dtc.seq - 1;
            CompletableFuture<ReadFrame<RefBuffer>> f2 = client.sendRequest(wf2, RefBufferDecoder.INSTANCE, new DtTime(1, TimeUnit.SECONDS));
            ReadFrame<RefBuffer> rf2 = f2.get(1, TimeUnit.SECONDS);
            Assertions.assertEquals(CmdCodes.SUCCESS, rf2.getRespCode());

            try {
                f1.get(1, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                assertEquals(NetException.class, e.getCause().getClass());
                assertTrue(e.getMessage().contains("dup seq"));
            }

        } finally {
            TestUtil.stop(client, server);
        }
    }

}

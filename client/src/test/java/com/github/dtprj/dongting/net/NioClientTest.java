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
import com.github.dtprj.dongting.common.TestUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.pb.DtFrame;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
public class NioClientTest {
    private static final DtLog log = DtLogs.getLogger(NioClientTest.class);

    private static class BioServer implements AutoCloseable {
        private ServerSocket ss;
        private Socket s;
        private DataInputStream in;
        private DataOutputStream out;
        private volatile boolean stop;
        private Thread readThread;
        private Thread writeThread;
        private ArrayBlockingQueue<DtFrame.Frame> queue = new ArrayBlockingQueue<>(100);
        private long sleep;
        private int resultCode = CmdCodes.SUCCESS;
        private String msg = "msg";

        public BioServer(int port) throws Exception {
            ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(port));
            new Thread(this::runAcceptThread).start();
        }

        public void runAcceptThread() {
            try {
                s = ss.accept();
                s.setSoTimeout(1000);
                readThread = new Thread(this::runReadThread);
                writeThread = new Thread(this::runWriteThread);
                readThread.start();
                writeThread.start();
            } catch (Throwable e) {
                log.error("", e);
            }
        }

        public void runReadThread() {
            try {
                in = new DataInputStream(s.getInputStream());
                while (!stop) {
                    int len = in.readInt();
                    byte[] data = new byte[len];
                    in.readFully(data);
                    DtFrame.Frame pbFrame = DtFrame.Frame.parseFrom(data);
                    queue.put(pbFrame);
                }
            } catch (EOFException e) {
            } catch (Exception e) {
                log.error("", e);
            }
        }

        public void runWriteThread() {
            try {
                out = new DataOutputStream(s.getOutputStream());
                while (!stop) {
                    if (queue.size() > 1) {
                        ArrayList<DtFrame.Frame> list = new ArrayList<>();
                        queue.drainTo(list);
                        // shuffle
                        for (int i = list.size() - 1; i >= 0; i--) {
                            writeFrame(out, list.get(i));
                        }
                    } else {
                        DtFrame.Frame frame = queue.take();
                        writeFrame(out, frame);
                    }
                }
            } catch (InterruptedException e) {
            } catch (Exception e) {
                log.error("", e);
            }
        }

        private void writeFrame(DataOutputStream out, DtFrame.Frame frame) throws Exception {
            frame = DtFrame.Frame.newBuilder().mergeFrom(frame)
                    .setFrameType(FrameType.TYPE_RESP)
                    .setRespCode(resultCode)
                    .setRespMsg(msg)
                    .build();
            byte[] bs = frame.toByteArray();
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
            out.writeInt(bs.length);
            out.write(bs);
        }

        @Override
        public void close() throws Exception {
            if (stop) {
                return;
            }
            stop = true;
            in.close();
            out.close();
            ss.close();
        }
    }

    @Test
    public void simpleTest() throws Exception {
        BioServer server = null;
        NioClient client = null;
        try {
            server = new BioServer(9000);
            NioClientConfig c = new NioClientConfig();
            c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
            client = new NioClient(c);
            client.start();
            client.waitStart();
            simpleTest(client, 100);
        } finally {
            CloseUtil.close(client, server);
        }
    }

    @Test
    public void multiServerTest() throws Exception {
        BioServer server1 = null;
        BioServer server2 = null;
        NioClient client = null;
        try {
            server1 = new BioServer(9000);
            server2 = new BioServer(9001);
            NioClientConfig c = new NioClientConfig();
            c.setHostPorts(Arrays.asList(new HostPort("127.0.0.1", 9000), new HostPort("127.0.0.1", 9001)));
            client = new NioClient(c);
            client.start();
            client.waitStart();
            simpleTest(client, 100);
        } finally {
            CloseUtil.close(client, server1, server2);
        }
    }

    private static void simpleTest(NioClient client, long timeMillis) throws Exception {
        final int maxBodySize = 5000;
        DtTime time = new DtTime();
        do {
            sendSync(maxBodySize, client, 500);
        } while (time.elapse(TimeUnit.MILLISECONDS) < timeMillis);
        time = new DtTime();
        CompletableFuture<Integer> successCount = new CompletableFuture<>();
        successCount.complete(0);
        int expectCount = 0;
        do {
            CompletableFuture<Void> f = sendAsync(maxBodySize, client, 500);
            successCount = successCount.thenCombine(f, (value, NULL) -> value + 1);
            expectCount++;
        } while (time.elapse(TimeUnit.MILLISECONDS) < timeMillis);
        int v = successCount.get(1, TimeUnit.SECONDS);
        assertTrue(v > 0);
        assertEquals(expectCount, v);
    }

    private static void sendSync(int maxBodySize, NioClient client, long timeoutMillis) throws Exception {
        ByteBufferWriteFrame wf = new ByteBufferWriteFrame();
        wf.setCommand(Commands.CMD_PING);
        wf.setFrameType(FrameType.TYPE_REQ);
        ThreadLocalRandom r = ThreadLocalRandom.current();
        byte[] bs = new byte[r.nextInt(maxBodySize)];
        r.nextBytes(bs);
        wf.setBody(ByteBuffer.wrap(bs));

        Decoder decoder;
        if (r.nextBoolean()) {
            decoder = ByteBufferDecoder.INSTANCE;
        } else {
            decoder = new BizByteBufferDecoder();
        }
        CompletableFuture<ReadFrame> f = client.sendRequest(wf,
                decoder, new DtTime(timeoutMillis, TimeUnit.MILLISECONDS));

        ReadFrame rf = f.get(5000, TimeUnit.MILLISECONDS);
        assertEquals(wf.getSeq(), rf.getSeq());
        assertEquals(FrameType.TYPE_RESP, rf.getFrameType());
        assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
        assertEquals("msg", rf.getMsg());
        ByteBuffer buf = (ByteBuffer) rf.getBody();
        byte[] respBody = new byte[buf.remaining()];
        buf.get(respBody);
        assertArrayEquals(bs, respBody);
    }

    private static void sendSyncByPeer(int maxBodySize, NioClient client,
                                       Peer peer, long timeoutMillis) throws Exception {
        ByteBufferWriteFrame wf = new ByteBufferWriteFrame();
        wf.setCommand(Commands.CMD_PING);
        wf.setFrameType(FrameType.TYPE_REQ);
        byte[] bs = new byte[ThreadLocalRandom.current().nextInt(maxBodySize)];
        ThreadLocalRandom.current().nextBytes(bs);
        wf.setBody(ByteBuffer.wrap(bs));
        CompletableFuture<ReadFrame> f = client.sendRequest(peer, wf,
                ByteBufferDecoder.INSTANCE, new DtTime(timeoutMillis, TimeUnit.MILLISECONDS));
        ReadFrame rf = f.get(5000, TimeUnit.MILLISECONDS);
        assertEquals(wf.getSeq(), rf.getSeq());
        assertEquals(FrameType.TYPE_RESP, rf.getFrameType());
        assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
        assertArrayEquals(bs, ((ByteBuffer) rf.getBody()).array());
    }

    private static CompletableFuture<Void> sendAsync(int maxBodySize, NioClient client, long timeoutMillis) {
        ByteBufferWriteFrame wf = new ByteBufferWriteFrame();
        wf.setCommand(Commands.CMD_PING);
        wf.setFrameType(FrameType.TYPE_REQ);
        ThreadLocalRandom r = ThreadLocalRandom.current();
        byte[] bs = new byte[r.nextInt(maxBodySize)];
        r.nextBytes(bs);
        wf.setBody(ByteBuffer.wrap(bs));
        CompletableFuture<ReadFrame> f = client.sendRequest(wf,
                ByteBufferDecoder.INSTANCE, new DtTime(timeoutMillis, TimeUnit.MILLISECONDS));
        return f.thenApply(rf -> {
            assertEquals(wf.getSeq(), rf.getSeq());
            assertEquals(FrameType.TYPE_RESP, rf.getFrameType());
            assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
            assertNotNull(rf.getBody());
            assertArrayEquals(bs, ((ByteBuffer) rf.getBody()).array());
            return null;
        });
    }

    @Test
    public void connectFailTest() {
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 23245)));
        c.setWaitStartTimeoutMillis(5);
        NioClient client = new NioClient(c);
        client.start();
        Assertions.assertThrows(NetException.class, () -> client.waitStart());
        client.stop();
    }

    @Test
    public void reconnectTest() throws Exception {
        BioServer server1 = null;
        BioServer server2 = null;
        NioClientConfig c = new NioClientConfig();
        c.setWaitStartTimeoutMillis(50);
        HostPort hp1 = new HostPort("127.0.0.1", 9000);
        HostPort hp2 = new HostPort("127.0.0.1", 9001);
        c.setHostPorts(Arrays.asList(hp1, hp2));
        NioClient client = new NioClient(c);
        try {
            server1 = new BioServer(9000);
            server2 = new BioServer(9001);

            client.start();
            client.waitStart();
            for (int i = 0; i < 10; i++) {
                sendSync(5000, client, 500);
            }
            server1.close();
            int success = 0;
            for (int i = 0; i < 10; i++) {
                try {
                    sendSync(5000, client, 100);
                    success++;
                } catch (Exception e) {
                }
            }
            assertTrue(success >= 9);

            Peer p1 = null;
            Peer p2 = null;
            for (Peer peer : client.getPeers()) {
                if (hp1.equals(peer.getEndPoint())) {
                    assertNull(peer.getDtChannel());
                    p1 = peer;
                } else {
                    assertNotNull(peer.getDtChannel());
                    p2 = peer;
                }
            }

            try {
                sendSyncByPeer(5000, client, p1, 500);
            } catch (ExecutionException e) {
                assertEquals(NetException.class, e.getCause().getClass());
            }
            sendSyncByPeer(5000, client, p2, 500);

            try {
                client.reconnect(p1).get(20, TimeUnit.MILLISECONDS);
                fail();
            } catch (TimeoutException | ExecutionException e) {
            }
            server1 = new BioServer(9000);
            client.reconnect(p1).get(200, TimeUnit.MILLISECONDS);
            sendSyncByPeer(5000, client, p1, 500);

            try {
                client.reconnect(p1).get(20, TimeUnit.MILLISECONDS);
                fail();
            } catch (ExecutionException e) {
                assertEquals(NetException.class, e.getCause().getClass());
            }

            Peer p = new Peer(null, null);
            try {
                client.reconnect(p);
                fail();
            } catch (IllegalArgumentException e) {
            }

            server1.close();
            server2.close();
            TestUtil.waitUtil(() -> client.getPeers().stream().allMatch(peer -> peer.getDtChannel() == null));
            try {
                sendSync(5000, client, 500);
            } catch (ExecutionException e) {
                assertEquals(NetException.class, e.getCause().getClass());
            }

        } finally {
            CloseUtil.close(client, server1, server2);
        }
    }

    @Test
    public void clientSemaphoreTimeoutTest() throws Exception {
        BioServer server = null;
        NioClient client = null;
        try {
            server = new BioServer(9000);
            server.sleep = 30;
            NioClientConfig c = new NioClientConfig();
            c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
            c.setCleanIntervalMills(1);
            c.setSelectTimeoutMillis(1);
            c.setMaxOutRequests(1);
            client = new NioClient(c);
            client.start();
            client.waitStart();
            CompletableFuture<Void> f1 = sendAsync(5000, client, 1000);
            CompletableFuture<Void> f2 = sendAsync(5000, client, 15);
            CompletableFuture<Void> f3 = sendAsync(5000, client, 1000);
            f1.get(1, TimeUnit.SECONDS);
            try {
                f2.get(1, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                assertEquals(NetTimeoutException.class, e.getCause().getClass());
            }
            f3.get(1, TimeUnit.SECONDS);
        } finally {
            CloseUtil.close(client, server);
        }
    }

    @Test
    public void clientStatusTest() throws Exception {
        BioServer server = null;
        NioClient client = null;
        try {
            server = new BioServer(9000);
            NioClientConfig c = new NioClientConfig();
            c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
            client = new NioClient(c);
            try {
                client.stop();
                fail();
            } catch (IllegalStateException e) {
            }

            try {
                sendSync(5000, client, 1000);
                fail();
            } catch (ExecutionException e) {
                assertEquals(IllegalStateException.class, e.getCause().getClass());
            }

            client.start();
            client.waitStart();
            sendSync(5000, client, 1000);

            client.stop();

            try {
                sendSync(5000, client, 1000);
                fail();
            } catch (ExecutionException e) {
                assertEquals(IllegalStateException.class, e.getCause().getClass());
            }

            try {
                client.start();
                fail();
            } catch (IllegalStateException e) {
            }
        } finally {
            CloseUtil.close(client, server);
        }
    }

    @Test
    public void errorCodeTest() throws Exception {
        BioServer server = null;
        NioClient client = null;
        try {
            server = new BioServer(9000);
            NioClientConfig c = new NioClientConfig();
            c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
            client = new NioClient(c);
            client.start();
            client.waitStart();
            server.resultCode = 100;
            try {
                sendSync(5000, client, 1000);
                fail();
            } catch (ExecutionException e) {
                assertEquals(NetCodeException.class, e.getCause().getClass());
                assertEquals(100, ((NetCodeException) e.getCause()).getCode());
            }
        } finally {
            CloseUtil.close(client, server);
        }
    }

    @Test
    public void closeTest1() throws Exception {
        BioServer server = null;
        NioClient client = null;
        try {
            server = new BioServer(9000);
            NioClientConfig c = new NioClientConfig();
            c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
            c.setCleanIntervalMills(1);
            c.setSelectTimeoutMillis(1);
            client = new NioClient(c);
            client.start();
            client.waitStart();
            server.sleep = 50;
            CompletableFuture<Void> f = sendAsync(3000, client, 1000);
            client.stop();
            f.get(1, TimeUnit.SECONDS);
        } finally {
            CloseUtil.close(client, server);
        }
    }

    @Test
    public void closeTest2() throws Exception {
        BioServer server = null;
        NioClient client = null;
        try {
            server = new BioServer(9000);
            NioClientConfig c = new NioClientConfig();
            c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
            c.setCleanIntervalMills(1);
            c.setSelectTimeoutMillis(0);
            c.setCloseTimeoutMillis(30);
            client = new NioClient(c);
            client.start();
            client.waitStart();
            server.sleep = 50;
            CompletableFuture<Void> f = sendAsync(3000, client, 1000);
            client.stop();
            try {
                f.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
            }
        } finally {
            CloseUtil.close(client, server);
        }
    }

    @Test
    public void badDecoderTest() throws Exception {
        BioServer server = null;
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        NioClient client = new NioClient(c);
        try {
            server = new BioServer(9000);
            client.start();
            client.waitStart();

            sendSync(5000, client, 1000);

            {
                // decoder fail in biz thread
                ByteBufferWriteFrame wf = new ByteBufferWriteFrame();
                wf.setCommand(Commands.CMD_PING);
                wf.setFrameType(FrameType.TYPE_REQ);
                wf.setBody(ByteBufferPool.EMPTY_BUFFER);

                Decoder decoder = new Decoder() {
                    @Override
                    public boolean decodeInIoThread() {
                        return false;
                    }

                    @Override
                    public Object decode(Object status, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
                        throw new ArrayIndexOutOfBoundsException();
                    }
                };
                CompletableFuture<ReadFrame> f = client.sendRequest(wf,
                        decoder, new DtTime(1, TimeUnit.SECONDS));

                try {
                    f.get(5000, TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    assertEquals(ArrayIndexOutOfBoundsException.class, e.getCause().getClass());
                }
            }
            {
                // decoder fail in io thread
                ByteBufferWriteFrame wf = new ByteBufferWriteFrame();
                wf.setCommand(Commands.CMD_PING);
                wf.setFrameType(FrameType.TYPE_REQ);
                wf.setBody(ByteBufferPool.EMPTY_BUFFER);
                Decoder decoder = new Decoder() {
                    @Override
                    public boolean decodeInIoThread() {
                        return true;
                    }

                    @Override
                    public Object decode(Object status, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
                        throw new ArrayIndexOutOfBoundsException();
                    }
                };
                CompletableFuture<ReadFrame> f = client.sendRequest(wf,
                        decoder, new DtTime(1, TimeUnit.SECONDS));

                try {
                    f.get(5000, TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    assertEquals(ArrayIndexOutOfBoundsException.class, e.getCause().getClass());
                }
            }

            // not affect the following requests
            for (int i = 0; i < 10; i++) {
                sendSync(5000, client, 1000);
            }
        } finally {
            CloseUtil.close(client, server);
        }
    }
}

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

import com.github.dtprj.dongting.buf.RefCountByteBuffer;
import com.github.dtprj.dongting.common.CloseUtil;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.TestUtil;
import com.github.dtprj.dongting.common.Tick;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.pb.DtFrame;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author huangli
 */
public class NioClientTest {
    private static final DtLog log = DtLogs.getLogger(NioClientTest.class);

    private static class BioServer implements AutoCloseable {
        private final ServerSocket ss;
        private final ArrayList<Socket> sockets = new ArrayList<>();
        private volatile boolean stop;
        private long sleep;
        private int resultCode = CmdCodes.SUCCESS;

        public BioServer(int port) throws Exception {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            new Thread(this::runAcceptThread).start();
        }

        public void runAcceptThread() {
            while (!stop) {
                try {
                    Socket s = ss.accept();
                    sockets.add(s);
                    s.setSoTimeout(1000);
                    ArrayBlockingQueue<DtFrame.Frame> queue = new ArrayBlockingQueue<>(100);
                    Thread readThread = new Thread(() -> runReadThread(s, queue));
                    Thread writeThread = new Thread(() -> runWriteThread(s, queue));
                    readThread.start();
                    writeThread.start();
                } catch (Throwable e) {
                    log.error("", e);
                }
            }
        }

        public void runReadThread(Socket s, ArrayBlockingQueue<DtFrame.Frame> queue) {
            DataInputStream in = null;
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
                // ignore
            } catch (Throwable e) {
                log.error("", e);
            } finally {
                CloseUtil.close(in);
            }
        }

        public void runWriteThread(Socket s, ArrayBlockingQueue<DtFrame.Frame> queue) {
            DataOutputStream out = null;
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
                // ignore
            } catch (Throwable e) {
                log.error("", e);
            } finally {
                CloseUtil.close(out);
            }
        }

        private void writeFrame(DataOutputStream out, DtFrame.Frame frame) throws Exception {
            frame = DtFrame.Frame.newBuilder().mergeFrom(frame)
                    .setFrameType(FrameType.TYPE_RESP)
                    .setRespCode(resultCode)
                    .setRespMsg("msg")
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
            for (Socket s : sockets) {
                CloseUtil.close(s);
            }
            CloseUtil.close(ss);
            // if no sleep, GitHub action fails: Bind Address already in use (Bind failed)
            Thread.sleep(1);
        }
    }

    @Test
    public void simpleSyncTest() throws Exception {
        BioServer server = null;
        NioClient client = null;
        try {
            server = new BioServer(9000);
            NioClientConfig c = new NioClientConfig();
            c.setReadBufferSize(2048);
            c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
            client = new NioClient(c);
            client.start();
            client.waitStart();
            sendSync(5000, client, 500);
        } finally {
            CloseUtil.close(client, server);
        }
    }

    @Test
    public void simpleAsyncTest() throws Exception {
        BioServer server = null;
        NioClient client = null;
        try {
            server = new BioServer(9000);
            NioClientConfig c = new NioClientConfig();
            c.setReadBufferSize(2048);
            c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
            client = new NioClient(c);
            client.start();
            client.waitStart();
            simpleAsyncTest(client, 200, 1, 6000);
        } finally {
            CloseUtil.close(client, server);
        }
    }

    @Test
    public void generalTest() throws Exception {
        BioServer server = null;
        NioClient client = null;
        try {
            server = new BioServer(9000);
            NioClientConfig c = new NioClientConfig();
            c.setReadBufferSize(2048);
            c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
            client = new NioClient(c);
            client.start();
            client.waitStart();
            generalTest(client, 100, 5000);
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
            c.setReadBufferSize(2048);
            c.setHostPorts(Arrays.asList(new HostPort("127.0.0.1", 9000), new HostPort("127.0.0.1", 9001)));
            client = new NioClient(c);
            client.start();
            client.waitStart();
            generalTest(client, 100, 5000);
        } finally {
            CloseUtil.close(client, server1, server2);
        }
    }

    private static void generalTest(NioClient client, long timeMillis, int maxBodySize) throws Exception {
        DtTime time = new DtTime();
        do {
            sendSync(maxBodySize, client, 500);
        } while (time.elapse(TimeUnit.MILLISECONDS) < timeMillis);

        simpleAsyncTest(client, timeMillis, Integer.MAX_VALUE, maxBodySize);
    }

    private static void simpleAsyncTest(NioClient client, long timeMillis, long maxLoop, int maxBodySize) throws Exception {
        DtTime time = new DtTime();
        CompletableFuture<Integer> successCount = new CompletableFuture<>();
        successCount.complete(0);
        int expectCount = 0;
        int loop = 0;
        do {
            CompletableFuture<Void> f = sendAsync(maxBodySize, client, 500);
            successCount = successCount.thenCombine(f, (value, NULL) -> value + 1);
            expectCount++;
            loop++;
        } while (time.elapse(TimeUnit.MILLISECONDS) < timeMillis && loop < maxLoop);
        int v = successCount.get(1, TimeUnit.SECONDS);
        assertTrue(v > 0);
        assertEquals(expectCount, v);
    }

    private static void sendSync(int maxBodySize, NioClient client, long timeoutMillis) throws Exception {
        sendSync(maxBodySize, client, timeoutMillis, new ByteBufferDecoder(maxBodySize / 2));
        sendSync(maxBodySize, client, timeoutMillis, new BizByteBufferDecoder());
        sendSync(maxBodySize, client, timeoutMillis, new IoFullPackByteBufferDecoder());
    }

    private static void sendSync(int maxBodySize, NioClient client, long timeoutMillis, Decoder decoder) throws Exception {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        byte[] bs = new byte[r.nextInt(maxBodySize)];
        r.nextBytes(bs);
        ByteBufferWriteFrame wf = new ByteBufferWriteFrame(ByteBuffer.wrap(bs));
        wf.setCommand(Commands.CMD_PING);
        wf.setFrameType(FrameType.TYPE_REQ);

        CompletableFuture<ReadFrame> f = client.sendRequest(wf,
                decoder, new DtTime(timeoutMillis, TimeUnit.MILLISECONDS));

        ReadFrame rf = f.get(5000, TimeUnit.MILLISECONDS);
        assertEquals(wf.getSeq(), rf.getSeq());
        assertEquals(FrameType.TYPE_RESP, rf.getFrameType());
        assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
        assertEquals("msg", rf.getMsg());
        if (bs.length != 0) {
            if (rf.getBody() instanceof RefCountByteBuffer) {
                RefCountByteBuffer rc = (RefCountByteBuffer) rf.getBody();
                assertEquals(ByteBuffer.wrap(bs), rc.getBuffer());
                rc.release();
            } else {
                ByteBuffer buf = (ByteBuffer) rf.getBody();
                assertEquals(ByteBuffer.wrap(bs), buf);
            }
        } else {
            assertNull(rf.getBody());
        }
    }

    private static void sendSyncByPeer(int maxBodySize, NioClient client,
                                       Peer peer, long timeoutMillis) throws Exception {
        byte[] bs = new byte[ThreadLocalRandom.current().nextInt(maxBodySize)];
        ThreadLocalRandom.current().nextBytes(bs);
        ByteBufferWriteFrame wf = new ByteBufferWriteFrame(ByteBuffer.wrap(bs));
        wf.setCommand(Commands.CMD_PING);
        wf.setFrameType(FrameType.TYPE_REQ);

        CompletableFuture<ReadFrame> f = client.sendRequest(peer, wf,
                new ByteBufferDecoder(maxBodySize / 2), new DtTime(timeoutMillis, TimeUnit.MILLISECONDS));
        ReadFrame rf = f.get(5000, TimeUnit.MILLISECONDS);
        assertEquals(wf.getSeq(), rf.getSeq());
        assertEquals(FrameType.TYPE_RESP, rf.getFrameType());
        assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
        if (bs.length != 0) {
            RefCountByteBuffer rc = (RefCountByteBuffer) rf.getBody();
            assertEquals(ByteBuffer.wrap(bs), rc.getBuffer());
            rc.release();
        } else {
            assertNull(rf.getBody());
        }
    }

    private static CompletableFuture<Void> sendAsync(int maxBodySize, NioClient client, long timeoutMillis) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        byte[] bs = new byte[r.nextInt(maxBodySize)];
        r.nextBytes(bs);
        ByteBufferWriteFrame wf = new ByteBufferWriteFrame(ByteBuffer.wrap(bs));
        wf.setCommand(Commands.CMD_PING);
        wf.setFrameType(FrameType.TYPE_REQ);

        CompletableFuture<ReadFrame> f = client.sendRequest(wf,
                new ByteBufferDecoder(maxBodySize / 2), new DtTime(timeoutMillis, TimeUnit.MILLISECONDS));
        return f.thenApply(rf -> {
            assertEquals(wf.getSeq(), rf.getSeq());
            assertEquals(FrameType.TYPE_RESP, rf.getFrameType());
            assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
            if (bs.length != 0) {
                RefCountByteBuffer rc = (RefCountByteBuffer) rf.getBody();
                assertEquals(ByteBuffer.wrap(bs), rc.getBuffer());
                rc.release();
            } else {
                assertNull(rf.getBody());
            }
            return null;
        });
    }

    @Test
    public void connectFailTest() {
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 23245)));
        c.setWaitStartTimeout(5);
        NioClient client = new NioClient(c);
        client.start();
        Assertions.assertThrows(NetException.class, client::waitStart);
        client.stop();
    }

    @Test
    public void reconnectTest() throws Exception {
        BioServer server1 = null;
        BioServer server2 = null;
        NioClientConfig c = new NioClientConfig();
        c.setWaitStartTimeout(50);
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
                    // ignore
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
                client.connect(p1, new DtTime(1, TimeUnit.SECONDS)).get(20, TimeUnit.MILLISECONDS);
                fail();
            } catch (TimeoutException | ExecutionException e) {
                // ignore
            }
            server1 = new BioServer(9000);
            client.connect(p1, new DtTime(1, TimeUnit.SECONDS)).get(200, TimeUnit.MILLISECONDS);
            sendSyncByPeer(5000, client, p1, 500);

            try {
                client.connect(p1, new DtTime(1, TimeUnit.SECONDS)).get(20, TimeUnit.MILLISECONDS);
                fail();
            } catch (ExecutionException e) {
                assertEquals(NetException.class, e.getCause().getClass());
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
    public void peerManageTest() throws Exception {
        BioServer server1 = null;
        BioServer server2 = null;
        NioClientConfig c = new NioClientConfig();
        c.setWaitStartTimeout(50);
        HostPort hp1 = new HostPort("127.0.0.1", 9000);
        HostPort hp2 = new HostPort("127.0.0.1", 9001);
        c.setHostPorts(new ArrayList<>());
        NioClient client = new NioClient(c);
        try {
            server1 = new BioServer(9000);
            server2 = new BioServer(9001);

            client.start();
            client.waitStart();
            Peer p1 = client.addPeer(hp1).get();
            Peer p2 = client.addPeer(hp2).get();
            assertSame(p1, client.addPeer(hp1).get());
            client.connect(p1, new DtTime(1, TimeUnit.SECONDS)).get();
            client.connect(p2, new DtTime(1, TimeUnit.SECONDS)).get();
            assertThrows(ExecutionException.class, () -> client.connect(p1, new DtTime(1, TimeUnit.SECONDS)).get());
            assertEquals(2, client.getPeers().size());

            sendSync(5000, client, 500);

            client.disconnect(p1).get();
            assertNull(p1.getDtChannel());
            assertEquals(2, client.getPeers().size());
            sendSync(5000, client, 100);

            client.connect(p1, new DtTime(1, TimeUnit.SECONDS)).get();
            assertNotNull(p1.getDtChannel());
            assertEquals(2, client.getPeers().size());
            sendSyncByPeer(5000, client, p1, 500);

            client.disconnect(p1).get();
            client.disconnect(p1).get();
            client.removePeer(p1).get();
            assertThrows(ExecutionException.class, () -> client.removePeer(p1).get());
            assertThrows(ExecutionException.class, () -> client.removePeer(p2).get());
            assertEquals(1, client.getPeers().size());
            sendSync(5000, client, 100);
            assertThrows(ExecutionException.class, () -> sendSyncByPeer(5000, client, p1, 500));
            sendSyncByPeer(5000, client, p2, 500);
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
            c.setCleanInterval(1);
            c.setSelectTimeout(1);
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
                // ignore
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
                // ignore
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
            c.setCleanInterval(0);
            c.setSelectTimeout(1);
            client = new NioClient(c);
            client.start();
            client.waitStart();
            server.sleep = Tick.tick(40);
            CompletableFuture<Void> f = sendAsync(3000, client, 1000);
            client.stop();
            f.get(1, TimeUnit.SECONDS);
        } finally {
            CloseUtil.close(client, server);
        }
    }

    @Test
    public void closeTest2() throws Exception {
        closeTest2Impl(0);
        closeTest2Impl(10);
    }

    private void closeTest2Impl(int closeTimeout) throws Exception {
        BioServer server = null;
        NioClient client = null;
        try {
            server = new BioServer(9000);
            NioClientConfig c = new NioClientConfig();
            c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
            c.setCleanInterval(0);
            c.setSelectTimeout(1);
            // close timeout less than server process time
            c.setCloseTimeout(Tick.tick(closeTimeout));
            client = new NioClient(c);
            client.start();
            client.waitStart();
            server.sleep = Tick.tick(40);
            CompletableFuture<Void> f = sendAsync(3000, client, 1000);
            client.stop();
            try {
                f.get(1, TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertEquals(NetException.class, e.getCause().getClass());
                String msg = e.getCause().getMessage();
                assertTrue(msg.contains("channel closed") || msg.equals("client closed"), msg);
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
                ByteBufferWriteFrame wf = new ByteBufferWriteFrame(ByteBuffer.allocate(1));
                wf.setCommand(Commands.CMD_PING);
                wf.setFrameType(FrameType.TYPE_REQ);

                Decoder decoder = new Decoder() {
                    @Override
                    public boolean decodeInIoThread() {
                        return false;
                    }

                    @Override
                    public Object decode(ChannelContext context, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
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
                ByteBufferWriteFrame wf = new ByteBufferWriteFrame(ByteBuffer.allocate(1));
                wf.setCommand(Commands.CMD_PING);
                wf.setFrameType(FrameType.TYPE_REQ);
                Decoder decoder = new Decoder() {
                    @Override
                    public boolean decodeInIoThread() {
                        return true;
                    }

                    @Override
                    public Object decode(ChannelContext context, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
                        throw new ArrayIndexOutOfBoundsException();
                    }
                };
                CompletableFuture<ReadFrame> f = client.sendRequest(wf,
                        decoder, new DtTime(1, TimeUnit.SECONDS));

                try {
                    f.get(5000, TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    e.printStackTrace();
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

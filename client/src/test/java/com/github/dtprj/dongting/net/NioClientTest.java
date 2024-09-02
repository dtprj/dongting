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
import com.github.dtprj.dongting.codec.ByteArrayDecoderCallback;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.DtPacket;
import com.github.dtprj.dongting.codec.RefBufferDecoderCallback;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.MockRuntimeException;
import com.github.dtprj.dongting.common.TestUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import org.junit.jupiter.api.AfterEach;
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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.dtprj.dongting.common.Tick.tick;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class NioClientTest {
    private static final DtLog log = DtLogs.getLogger(NioClientTest.class);

    private BioServer server1;
    private BioServer server2;
    private NioClient client;

    private static class BioServer implements AutoCloseable {
        private final ServerSocket ss;
        private final ArrayList<Socket> sockets = new ArrayList<>();
        private volatile boolean stop;
        private long sleep;
        private int resultCode = CmdCodes.SUCCESS;

        public BioServer(int port) throws Exception {
            ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(port));
            new Thread(this::runAcceptThread).start();
        }

        public void runAcceptThread() {
            while (!stop) {
                try {
                    Socket s = ss.accept();
                    sockets.add(s);
                    s.setSoTimeout(tick(1000));
                    ArrayBlockingQueue<DtPacket.Packet> queue = new ArrayBlockingQueue<>(100);
                    Thread readThread = new Thread(() -> runReadThread(s, queue));
                    Thread writeThread = new Thread(() -> runWriteThread(s, queue));
                    readThread.start();
                    writeThread.start();
                } catch (Throwable e) {
                    log.error("", e);
                }
            }
        }

        public void runReadThread(Socket s, ArrayBlockingQueue<DtPacket.Packet> queue) {
            DataInputStream in = null;
            try {
                in = new DataInputStream(s.getInputStream());
                while (!stop) {
                    int len = in.readInt();
                    byte[] data = new byte[len];
                    in.readFully(data);
                    DtPacket.Packet pbPacket = DtPacket.Packet.parseFrom(data);
                    queue.put(pbPacket);
                }
            } catch (EOFException e) {
                // ignore
            } catch (Throwable e) {
                log.error("", e);
            } finally {
                DtUtil.close(in);
            }
        }

        public void runWriteThread(Socket s, ArrayBlockingQueue<DtPacket.Packet> queue) {
            DataOutputStream out = null;
            try {
                out = new DataOutputStream(s.getOutputStream());
                while (!stop) {
                    if (queue.size() > 1) {
                        ArrayList<DtPacket.Packet> list = new ArrayList<>();
                        queue.drainTo(list);
                        // shuffle
                        for (int i = list.size() - 1; i >= 0; i--) {
                            writePacket(out, list.get(i));
                        }
                    } else {
                        DtPacket.Packet packet = queue.take();
                        writePacket(out, packet);
                    }
                }
            } catch (InterruptedException e) {
                // ignore
            } catch (Throwable e) {
                log.error("", e);
            } finally {
                DtUtil.close(out);
            }
        }

        private void writePacket(DataOutputStream out, DtPacket.Packet packet) throws Exception {
            packet = DtPacket.Packet.newBuilder().mergeFrom(packet)
                    .setPacketType(PacketType.TYPE_RESP)
                    .setRespCode(resultCode)
                    .setRespMsg("msg")
                    .build();
            byte[] bs = packet.toByteArray();
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
                DtUtil.close(s);
            }
            DtUtil.close(ss);
            // if no sleep, GitHub action fails: Bind Address already in use (Bind failed)
            Thread.sleep(tick(2));
        }
    }

    @AfterEach
    public void afterTest() {
        TestUtil.stop(client);
        DtUtil.close(server1, server2);
        client = null;
        server1 = null;
        server2 = null;
    }

    @Test
    public void simpleSyncTest() throws Exception {
        server1 = new BioServer(9000);
        NioClientConfig c = new NioClientConfig();
        c.setReadBufferSize(2048);
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        client = new NioClient(c);
        client.start();
        client.waitStart();
        sendSync(5000, client, tick(1000));
    }

    @Test
    public void simpleAsyncTest() throws Exception {
        server1 = new BioServer(9000);
        NioClientConfig c = new NioClientConfig();
        c.setReadBufferSize(2048);
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        client = new NioClient(c);
        client.start();
        client.waitStart();
        asyncTest(client, tick(1000), 1, 6000);
    }

    @Test
    public void generalTest() throws Exception {
        server1 = new BioServer(9000);
        NioClientConfig c = new NioClientConfig();
        c.setReadBufferSize(2048);
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        client = new NioClient(c);
        client.start();
        client.waitStart();
        generalTest(client, tick(100));
    }

    private static void generalTest(NioClient client, long timeMillis) throws Exception {
        int maxBodySize = 5000;
        DtTime time = new DtTime();
        do {
            sendSync(maxBodySize, client, tick(500));
        } while (time.elapse(TimeUnit.MILLISECONDS) < timeMillis);

        asyncTest(client, timeMillis, Integer.MAX_VALUE, maxBodySize);
    }

    private static void asyncTest(NioClient client, long timeMillis, long maxLoop, int maxBodySize) throws Exception {
        DtTime time = new DtTime();
        CompletableFuture<Integer> successCount = new CompletableFuture<>();
        successCount.complete(0);
        int expectCount = 0;
        int loop = 0;
        do {
            CompletableFuture<Void> f = sendAsync(maxBodySize, client, tick(500));
            successCount = successCount.thenCombine(f, (value, NULL) -> value + 1);
            expectCount++;
            loop++;
        } while (time.elapse(TimeUnit.MILLISECONDS) < timeMillis && loop < maxLoop);
        int v = successCount.get(tick(1), TimeUnit.SECONDS);
        assertTrue(v > 0);
        assertEquals(expectCount, v);
    }

    @Test
    public void multiServerTest() throws Exception {
        server1 = new BioServer(9000);
        server2 = new BioServer(9001);
        NioClientConfig c = new NioClientConfig();
        c.setReadBufferSize(2048);
        c.setHostPorts(Arrays.asList(new HostPort("127.0.0.1", 9000), new HostPort("127.0.0.1", 9001)));
        client = new NioClient(c);
        client.start();
        client.waitStart();
        generalTest(client, tick(100));
    }

    private static void sendSync(int maxBodySize, NioClient client, long timeoutMillis) throws Exception {
        sendSync(maxBodySize, client, timeoutMillis, new RefBufferDecoderCallback());
        sendSync(maxBodySize, client, timeoutMillis, new IoFullPackByteBufferDecoderCallback());
    }

    private static void sendSync(int maxBodySize, NioClient client, long timeoutMillis, DecoderCallback<?> decoderCallback) throws Exception {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        byte[] bs = new byte[r.nextInt(maxBodySize)];
        r.nextBytes(bs);
        ByteBufferWritePacket wf = new ByteBufferWritePacket(ByteBuffer.wrap(bs));
        wf.setCommand(Commands.CMD_PING);

        CompletableFuture<?> f = client.sendRequest(wf,
                ctx -> decoderCallback, new DtTime(timeoutMillis, TimeUnit.MILLISECONDS));

        ReadPacket<?> rf = (ReadPacket<?>) f.get(5000, TimeUnit.MILLISECONDS);
        assertEquals(wf.getSeq(), rf.getSeq());
        assertEquals(PacketType.TYPE_RESP, rf.getPacketType());
        assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
        assertEquals("msg", rf.getMsg());
        if (rf.getBody() instanceof RefBuffer) {
            RefBuffer rc = (RefBuffer) rf.getBody();
            assertEquals(ByteBuffer.wrap(bs), rc.getBuffer());
            rc.release();
        } else {
            ByteBuffer buf = (ByteBuffer) rf.getBody();
            assertEquals(ByteBuffer.wrap(bs), buf);
        }
    }

    private static void sendSyncByPeer(int maxBodySize, NioClient client,
                                       Peer peer, long timeoutMillis) throws Exception {
        byte[] bs = new byte[ThreadLocalRandom.current().nextInt(maxBodySize)];
        ThreadLocalRandom.current().nextBytes(bs);
        ByteBufferWritePacket wf = new ByteBufferWritePacket(ByteBuffer.wrap(bs));
        wf.setCommand(Commands.CMD_PING);

        CompletableFuture<ReadPacket<RefBuffer>> f = client.sendRequest(peer, wf,
                ctx -> new RefBufferDecoderCallback(), new DtTime(timeoutMillis, TimeUnit.MILLISECONDS));
        ReadPacket<RefBuffer> rf = f.get(5000, TimeUnit.MILLISECONDS);
        assertEquals(wf.getSeq(), rf.getSeq());
        assertEquals(PacketType.TYPE_RESP, rf.getPacketType());
        assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
        RefBuffer rc = rf.getBody();
        assertEquals(ByteBuffer.wrap(bs), rc.getBuffer());
        rc.release();
    }

    private static CompletableFuture<Void> sendAsync(int maxBodySize, NioClient client, long timeoutMillis) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        byte[] bs = new byte[r.nextInt(maxBodySize)];
        r.nextBytes(bs);
        ByteBufferWritePacket wf = new ByteBufferWritePacket(ByteBuffer.wrap(bs));
        wf.setCommand(Commands.CMD_PING);

        CompletableFuture<ReadPacket<RefBuffer>> f = client.sendRequest(wf,
                ctx -> new RefBufferDecoderCallback(), new DtTime(timeoutMillis, TimeUnit.MILLISECONDS));
        return f.thenApply(rf -> {
            assertEquals(wf.getSeq(), rf.getSeq());
            assertEquals(PacketType.TYPE_RESP, rf.getPacketType());
            assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
            RefBuffer rc = rf.getBody();
            assertEquals(ByteBuffer.wrap(bs), rc.getBuffer());
            rc.release();
            return null;
        });
    }

    @Test
    public void connectFailTest() {
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 23245)));
        c.setWaitStartTimeout(tick(10));
        client = new NioClient(c);
        client.start();
        Assertions.assertThrows(NetException.class, client::waitStart);
    }

    @Test
    public void reconnectTest1() throws Exception {
        NioClientConfig c = new NioClientConfig();
        c.setWaitStartTimeout(tick(100));
        HostPort hp1 = new HostPort("127.0.0.1", 9000);
        HostPort hp2 = new HostPort("127.0.0.1", 9001);
        c.setHostPorts(Arrays.asList(hp1, hp2));
        client = new NioClient(c);

        server1 = new BioServer(9000);
        server2 = new BioServer(9001);

        client.start();
        client.waitStart();
        for (int i = 0; i < 10; i++) {
            sendSync(5000, client, tick(500));
        }
        DtUtil.close(server1);
        int success = 0;
        for (int i = 0; i < 10; i++) {
            try {
                sendSync(5000, client, tick(100));
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
            sendSyncByPeer(5000, client, p1, tick(500));
        } catch (ExecutionException e) {
            assertEquals(NetException.class, e.getCause().getClass());
        }
        sendSyncByPeer(5000, client, p2, tick(500));

        try {
            client.connect(p1, new DtTime(1, TimeUnit.SECONDS)).get(tick(20), TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException | ExecutionException e) {
            // ignore
        }
        server1 = new BioServer(9000);
        client.connect(p1, new DtTime(1, TimeUnit.SECONDS));
        // connect is idempotent
        client.connect(p1, new DtTime(1, TimeUnit.SECONDS)).get(tick(20), TimeUnit.MILLISECONDS);
        sendSyncByPeer(5000, client, p1, 500);

        // connect is idempotent
        client.connect(p1, new DtTime(1, TimeUnit.SECONDS)).get(tick(20), TimeUnit.MILLISECONDS);

        DtUtil.close(server1, server2);
        server1 = null;
        server2 = null;
        TestUtil.waitUtil(() -> client.getPeers().stream().allMatch(peer -> peer.getDtChannel() == null));
        try {
            sendSync(5000, client, tick(500));
        } catch (ExecutionException e) {
            assertEquals(NetException.class, e.getCause().getClass());
        }
    }

    @Test
    public void reconnectTest2() throws Exception {
        NioClientConfig c = new NioClientConfig();
        c.setWaitStartTimeout(tick(100));
        HostPort hp1 = new HostPort("127.0.0.1", 9000);
        c.setHostPorts(List.of(hp1));
        client = new NioClient(c);

        server1 = new BioServer(9000);

        client.start();
        client.waitStart();

        DtUtil.close(server1);

        Peer p1 = client.getPeers().get(0);

        try {
            sendSyncByPeer(5000, client, p1, tick(500));
        } catch (ExecutionException e) {
            assertEquals(NetException.class, e.getCause().getClass());
        }

        server1 = new BioServer(9000);

        // auto connect
        sendSyncByPeer(5000, client, p1, 500);

    }

    @Test
    public void peerManageTest() throws Exception {
        NioClientConfig c = new NioClientConfig();
        c.setWaitStartTimeout(tick(50));
        HostPort hp1 = new HostPort("127.0.0.1", 9000);
        HostPort hp2 = new HostPort("127.0.0.1", 9001);
        c.setHostPorts(new ArrayList<>());
        client = new NioClient(c);

        server1 = new BioServer(9000);
        server2 = new BioServer(9001);

        client.start();
        client.waitStart();
        Peer p1 = client.addPeer(hp1).get();
        Peer p2 = client.addPeer(hp2).get();
        assertSame(p1, client.addPeer(hp1).get());
        client.connect(p1, new DtTime(tick(1), TimeUnit.SECONDS)).get();
        client.connect(p2, new DtTime(tick(1), TimeUnit.SECONDS)).get();
        // connect is idempotent
        client.connect(p1, new DtTime(tick(1), TimeUnit.SECONDS)).get();
        client.connect(p2, new DtTime(tick(1), TimeUnit.SECONDS)).get();
        assertEquals(2, client.getPeers().size());

        sendSync(5000, client, tick(500));

        client.disconnect(p1).get();
        assertNull(p1.getDtChannel());
        assertEquals(2, client.getPeers().size());
        sendSync(5000, client, tick(100));

        client.connect(p1, new DtTime(1, TimeUnit.SECONDS)).get();
        assertNotNull(p1.getDtChannel());
        assertEquals(2, client.getPeers().size());
        sendSyncByPeer(5000, client, p1, tick(500));

        client.disconnect(p1).get();
        client.disconnect(p1).get();
        client.removePeer(p1).get();
        client.removePeer(p1).get(); //idempotent

        try {
            client.connect(p1, new DtTime(1, TimeUnit.SECONDS)).get();
        } catch (ExecutionException e) {
            assertEquals(NetException.class, e.getCause().getClass());
            assertEquals("peer is removed", e.getCause().getMessage());
        }

        assertEquals(1, client.getPeers().size());
        sendSync(5000, client, tick(100));
        assertThrows(ExecutionException.class, () -> sendSyncByPeer(5000, client, p1, tick(500)));
        sendSyncByPeer(5000, client, p2, tick(500));

        client.removePeer(p2.getEndPoint()).get();

    }

    @Test
    public void clientSemaphoreTimeoutTest() throws Exception {

        server1 = new BioServer(9000);
        server1.sleep = tick(30);
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        c.setCleanInterval(1);
        c.setSelectTimeout(1);
        c.setMaxOutRequests(1);
        client = new NioClient(c);
        client.start();
        client.waitStart();
        CompletableFuture<Void> f1 = sendAsync(5000, client, tick(1000));
        CompletableFuture<Void> f2 = sendAsync(5000, client, tick(15));
        CompletableFuture<Void> f3 = sendAsync(5000, client, tick(1000));
        f1.get(tick(1), TimeUnit.SECONDS);
        try {
            f2.get(tick(1), TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
        }
        f3.get(tick(1), TimeUnit.SECONDS);

    }

    @Test
    public void clientStatusTest() throws Exception {
        server1 = new BioServer(9000);
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        client = new NioClient(c);
        client.stop(new DtTime(1, TimeUnit.SECONDS));
        assertEquals(AbstractLifeCircle.STATUS_STOPPED, client.getStatus());

        try {
            sendSync(5000, client, tick(1000));
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetException.class, e.getCause().getClass());
        }

        client = new NioClient(c);
        client.start();
        client.waitStart();
        sendSync(5000, client, tick(1000));

        TestUtil.stop(client);

        try {
            sendSync(5000, client, tick(1000));
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetException.class, e.getCause().getClass());
        }

        try {
            client.start();
            fail();
        } catch (IllegalStateException e) {
            // ignore
        }
    }

    @Test
    public void errorCodeTest() throws Exception {
        server1 = new BioServer(9000);
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        client = new NioClient(c);
        client.start();
        client.waitStart();
        server1.resultCode = 100;
        try {
            sendSync(5000, client, tick(1000));
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetCodeException.class, e.getCause().getClass());
            assertEquals(100, ((NetCodeException) e.getCause()).getCode());
        }
    }

    @Test
    public void closeTest1() throws Exception {
        server1 = new BioServer(9000);
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        c.setCleanInterval(0);
        c.setSelectTimeout(1);
        client = new NioClient(c);
        client.start();
        client.waitStart();
        server1.sleep = tick(40);
        CompletableFuture<Void> f = sendAsync(3000, client, tick(1000));
        client.stop(new DtTime(3, TimeUnit.SECONDS));
        f.get(tick(1), TimeUnit.SECONDS);
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
            client = new NioClient(c);
            client.start();
            client.waitStart();
            server.sleep = tick(40);
            CompletableFuture<Void> f = sendAsync(3000, client, tick(1000));
            // close timeout less than server process time
            client.stop(new DtTime(tick(closeTimeout), TimeUnit.MILLISECONDS));
            try {
                f.get(tick(1), TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertEquals(NetException.class, e.getCause().getClass());
                String msg = e.getCause().getMessage();
                assertTrue(msg.contains("channel closed, cancel request still in IoChannelQueue") ||
                        msg.contains("client closed"), msg);
            }
        } finally {
            TestUtil.stop(client);
            DtUtil.close(server);
        }
    }

    @Test
    public void waitAutoConnectTimeoutTest() throws Exception {
        NioClientConfig c = new NioClientConfig();
        c.setCleanInterval(0);
        c.setSelectTimeout(1);
        client = new NioClient(c);
        client.start();
        client.waitStart();
        Peer peer = client.addPeer(new HostPort("110.110.110.110", 2345)).get();

        try {
            // auto connect
            sendSyncByPeer(5000, client, peer, 1);
            fail();
        } catch (ExecutionException e) {
            assertEquals(NetTimeoutException.class, e.getCause().getClass());
            assertEquals("wait connect timeout", e.getCause().getMessage());
        }
    }

    @Test
    public void failOrderTest() throws Exception {
        server1 = new BioServer(9000);
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        c.setCleanInterval(0);
        c.setSelectTimeout(1);
        c.setFinishPendingImmediatelyWhenChannelClose(true);
        client = new NioClient(c);
        client.start();
        client.waitStart();
        server1.sleep = tick(300);

        byte[] bs = new byte[1];

        AtomicInteger expectFinishIndex = new AtomicInteger(0);
        AtomicReference<Throwable> fail = new AtomicReference<>();
        int loop = 40;
        //noinspection rawtypes
        CompletableFuture[] allFutures = new CompletableFuture[loop];
        for (int i = 0; i < loop; i++) {
            ByteBufferWritePacket wf = new ByteBufferWritePacket(ByteBuffer.wrap(bs));
            wf.setCommand(Commands.CMD_PING);
            CompletableFuture<?> f = client.sendRequest(wf, ctx -> new RefBufferDecoderCallback(),
                    new DtTime(100, TimeUnit.SECONDS));
            int index = i;
            allFutures[i] = f.handle((v, e) -> {
                if (e == null) {
                    fail.compareAndSet(null, e);
                    return null;
                }
                int expectIndex = expectFinishIndex.getAndIncrement();
                if (expectIndex != index) {
                    fail.compareAndSet(null, new Exception("fail order " + index + "," + expectIndex
                            + ", msg=" + e.getMessage()));
                }
                return null;
            });
        }

        client.stop(new DtTime(1, TimeUnit.NANOSECONDS));
        client = null;
        CompletableFuture.allOf(allFutures).get();
        assertNull(fail.get());
    }

    @Test
    public void badDecoderTest() throws Exception {
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        client = new NioClient(c);
        server1 = new BioServer(9000);
        client.start();
        client.waitStart();

        sendSync(5000, client, tick(1000));

        {
            // decoder fail in io thread
            ByteBufferWritePacket wf = new ByteBufferWritePacket(ByteBuffer.allocate(1));
            wf.setCommand(Commands.CMD_PING);
            DecoderCallback<Object> decoderCallback = new DecoderCallback<Object>() {

                @Override
                protected boolean doDecode(ByteBuffer buffer, int bodyLen, int currentPos) {
                    throw new MockRuntimeException();
                }

                @Override
                protected Object getResult() {
                    return null;
                }
            };
            CompletableFuture<?> f = client.sendRequest(wf,
                    ctx -> decoderCallback, new DtTime(tick(1), TimeUnit.SECONDS));

            try {
                f.get(tick(5), TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertEquals(MockRuntimeException.class, e.getCause().getClass());
            }
        }
        TestUtil.waitUtil(() -> client.getPeers().get(0).getStatus() == PeerStatus.not_connect);
    }

    @Test
    public void badEncoderTest() throws Exception {
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        client = new NioClient(c);
        server1 = new BioServer(9000);
        client.start();
        client.waitStart();

        sendSync(5000, client, tick(1000));

        {
            // encode fail in io thread
            WritePacket wf = new WritePacket() {
                @Override
                protected int calcActualBodySize() {
                    throw new MockRuntimeException();
                }

                @Override
                protected boolean encodeBody(RpcEncodeContext context, ByteBuffer dest) {
                    return true;
                }
            };
            wf.setCommand(Commands.CMD_PING);
            CompletableFuture<?> f = client.sendRequest(wf, ctx -> new ByteArrayDecoderCallback(),
                    new DtTime(tick(1), TimeUnit.SECONDS));

            try {
                f.get(tick(5), TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertEquals(MockRuntimeException.class, DtUtil.rootCause(e).getClass());
            }
        }

        // not affect the following requests
        for (int i = 0; i < 10; i++) {
            sendSync(5000, client, tick(1000));
        }

        {
            // encode fail in io thread
            WritePacket wf = new WritePacket() {
                @Override
                protected int calcActualBodySize() {
                    return 1;
                }

                @Override
                protected boolean encodeBody(RpcEncodeContext context, ByteBuffer dest) {
                    throw new MockRuntimeException();
                }
            };
            wf.setCommand(Commands.CMD_PING);
            CompletableFuture<?> f = client.sendRequest(wf, ctx -> new ByteArrayDecoderCallback(),
                    new DtTime(tick(1), TimeUnit.SECONDS));

            try {
                f.get(tick(5), TimeUnit.SECONDS);
                fail();
            } catch (ExecutionException e) {
                assertTrue(e.getMessage().contains("channel closed, cancel request"));
            }
        }

        // not affect the following requests
        for (int i = 0; i < 10; i++) {
            sendSyncByPeer(5000, client, client.getPeers().get(0), tick(1000));
        }
    }

    @Test
    public void largePacketTest() throws Exception {
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 9000)));
        c.setMaxPacketSize(100 + 128 * 1024);
        c.setMaxBodySize(100);
        client = new NioClient(c);
        server1 = new BioServer(9000);
        client.start();
        client.waitStart();

        Peer peer = client.getPeers().get(0);

        ByteBuffer buf = ByteBuffer.allocate(101 + 128 * 1024);
        try {
            ByteBufferWritePacket f = new ByteBufferWritePacket(buf);
            f.setCommand(Commands.CMD_PING);
            client.sendRequest(peer, f, ctx -> new ByteArrayDecoderCallback(), new DtTime(1, TimeUnit.SECONDS)).get();
            fail();
        } catch (Exception e) {
            assertEquals(NetException.class, DtUtil.rootCause(e).getClass());
            assertEquals("estimateSize overflow", DtUtil.rootCause(e).getMessage());
        }

        buf.limit(101);
        try {
            ByteBufferWritePacket f = new ByteBufferWritePacket(buf);
            f.setCommand(Commands.CMD_PING);
            client.sendRequest(peer, f, ctx -> new ByteArrayDecoderCallback(), new DtTime(1, TimeUnit.SECONDS)).get();
            fail();
        } catch (Exception e) {
            assertEquals(NetException.class, DtUtil.rootCause(e).getClass());
            assertTrue(DtUtil.rootCause(e).getMessage().contains("exceeds max body size"));
        }

        sendSyncByPeer(100, client, peer, tick(1000));

    }
}

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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.pb.DtFrame;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class NioClientTest {
    private static final DtLog log = DtLogs.getLogger(NioClientTest.class);

    private static class BioServer implements AutoCloseable {
        private ServerSocket ss;
        private Socket s;
        private volatile boolean stop;
        private Thread readThread;
        private Thread writeThread;
        private ArrayBlockingQueue<DtFrame.Frame> queue = new ArrayBlockingQueue<>(100);

        public BioServer(int port) throws Exception {
            ss = new ServerSocket(port);
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
                DataInputStream in = new DataInputStream(s.getInputStream());
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
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
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

        private static void writeFrame(DataOutputStream out, DtFrame.Frame frame) throws IOException {
            frame = DtFrame.Frame.newBuilder().mergeFrom(frame)
                    .setFrameType(CmdType.TYPE_RESP)
                    .build();
            byte[] bs = frame.toByteArray();
            out.writeInt(bs.length);
            out.write(bs);
        }

        @Override
        public void close() throws Exception {
            stop = true;
            readThread.interrupt();
            readThread.join(1000);
            writeThread.interrupt();
            writeThread.join(1000);
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
            simpleTest(client);
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
            simpleTest(client);
        } finally {
            CloseUtil.close(client, server1, server2);
        }
    }

    private static void simpleTest(NioClient client) throws Exception {
        int seq = 0;
        final int maxBodySize = 5000;
        Random r = new Random();
        DtTime time = new DtTime();
        while (time.elapse(TimeUnit.MILLISECONDS) < 100) {
            ByteBufferWriteFrame wf = new ByteBufferWriteFrame();
            wf.setCommand(Commands.CMD_PING);
            wf.setFrameType(CmdType.TYPE_REQ);
            wf.setSeq(seq++);
            byte[] bs = new byte[r.nextInt(maxBodySize)];
            r.nextBytes(bs);
            wf.setBody(ByteBuffer.wrap(bs));
            CompletableFuture<ReadFrame> f = client.sendRequest(wf,
                    ByteBufferDecoder.INSTANCE, new DtTime(1, TimeUnit.SECONDS));
            ReadFrame rf = f.get(1000, TimeUnit.MILLISECONDS);
            assertEquals(wf.getSeq(), rf.getSeq());
            assertEquals(CmdType.TYPE_RESP, rf.getFrameType());
            assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
            assertArrayEquals(bs, ((ByteBuffer) rf.getBody()).array());
        }
        time = new DtTime();
        CompletableFuture<Integer> successCount = new CompletableFuture<>();
        successCount.complete(0);
        int expectCount = 0;
        while (time.elapse(TimeUnit.MILLISECONDS) < 100) {
            ByteBufferWriteFrame wf = new ByteBufferWriteFrame();
            wf.setCommand(Commands.CMD_PING);
            wf.setFrameType(CmdType.TYPE_REQ);
            wf.setSeq(seq++);
            byte[] bs = new byte[r.nextInt(maxBodySize)];
            r.nextBytes(bs);
            wf.setBody(ByteBuffer.wrap(bs));
            CompletableFuture<ReadFrame> f = client.sendRequest(wf,
                    ByteBufferDecoder.INSTANCE, new DtTime(1, TimeUnit.SECONDS));
            expectCount++;
            successCount = successCount.thenCombine(f, (currentCount, rf) -> {
                try {
                    assertEquals(wf.getSeq(), rf.getSeq());
                    assertEquals(CmdType.TYPE_RESP, rf.getFrameType());
                    assertEquals(CmdCodes.SUCCESS, rf.getRespCode());
                    assertArrayEquals(bs, ((ByteBuffer) rf.getBody()).array());
                    return currentCount + 1;
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            });
        }
        int v = successCount.get(1, TimeUnit.SECONDS);
        assertTrue(v > 0);
        assertEquals(expectCount, v);
    }

    @Test
    public void connectFailTest() {
        NioClientConfig c = new NioClientConfig();
        c.setHostPorts(Collections.singletonList(new HostPort("127.0.0.1", 23245)));
        c.setConnectTimeoutMillis(10);
        NioClient client = new NioClient(c);
        client.start();
        Assertions.assertThrows(NetException.class, () -> client.waitStart());
        client.stop();
    }
}

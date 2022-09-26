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
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.pb.DtFrame;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
public class NioServerTest {
    private static final int CMD_IO_PING = 3000;
    private static final int CMD_BIZ_PING = 3001;

    private static final int PORT = 9000;

    private NioServer server;

    private void setupServer(Consumer<NioServerConfig> consumer) throws Exception {
        NioServerConfig c = new NioServerConfig();
        c.setPort(PORT);
        if (consumer != null) {
            consumer.accept(c);
        }
        server = new NioServer(c);
        server.register(CMD_IO_PING, new NioServer.PingProcessor(true));
        server.register(CMD_BIZ_PING, new NioServer.PingProcessor(false));
        server.start();
    }

    @AfterEach
    public void teardown() throws Exception {
        server.stop();
    }

    @Test
    public void simpleTest() throws Exception {
        setupServer(null);
        simpleTest(1000, 0, 5000, null);
    }

    private void simpleTest(long millis, int innerLoop, int maxBodySize,
                            BiConsumer<DataOutputStream, byte[]> writer) throws Exception {
        Socket s = new Socket("127.0.0.1", PORT);
        try {
            s.setSoTimeout(1000);
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            Random r = new Random();
            DtTime t = new DtTime();
            int seq = 0;
            while (t.elapse(TimeUnit.MILLISECONDS) < millis) {
                int count = innerLoop <= 0 ? r.nextInt(10) + 1 : innerLoop;
                HashMap<Integer, byte[]> map = new HashMap<>();
                for (int i = 0; i < count; i++) {
                    if (r.nextDouble() < 0.05) {
                        //empty test
                        map.put(seq + i, new byte[]{});
                    } else {
                        byte[] bs = new byte[r.nextInt(maxBodySize)];
                        r.nextBytes(bs);
                        map.put(seq + i, bs);
                    }
                }
                for (int i = 0; i < count; i++) {
                    DtFrame.Frame frame = DtFrame.Frame.newBuilder().setFrameType(CmdType.TYPE_REQ)
                            .setSeq(seq + i)
                            .setCommand(r.nextBoolean() ? CMD_IO_PING : CMD_BIZ_PING)
                            .setBody(ByteString.copyFrom(map.get(seq + i)))
                            .build();
                    byte[] bs = frame.toByteArray();
                    if (writer == null) {
                        out.writeInt(bs.length);
                        out.write(bs);
                    } else {
                        writer.accept(out, bs);
                    }
                }
                seq += count;
                out.flush();
                for (int i = 0; i < count; i++) {
                    int len = in.readInt();
                    byte[] resp = new byte[len];
                    in.readFully(resp);
                    DtFrame.Frame frame = DtFrame.Frame.parseFrom(resp);
                    assertEquals(CmdType.TYPE_RESP, frame.getFrameType());
                    assertEquals(CmdCodes.SUCCESS, frame.getRespCode());
                    assertArrayEquals(map.get(frame.getSeq()), frame.getBody().toByteArray());
                }
            }
            s.close();
        } finally {
            CloseUtil.close(s);
        }
    }

    @Test
    public void multiClientTest() throws Exception {
        setupServer(null);
        final int threads = 200;
        AtomicInteger fail = new AtomicInteger(0);
        Thread[] ts = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            Runnable r = () -> {
                try {
                    simpleTest(1000, 0, 5000, null);
                } catch (Throwable e) {
                    BugLog.log(e);
                    fail.incrementAndGet();
                }
            };
            ts[i] = new Thread(r);
            ts[i].start();
        }
        for (int i = 0; i < threads; i++) {
            ts[i].join(10000);
        }
        assertEquals(0, fail.get());
    }

    @Test
    public void clientReadBlockTest() throws Exception {
        setupServer(null);
        simpleTest(10, 500, 1 * 1024 * 1024, null);
    }

    @Test
    public void clientSlowWriteTest() throws Exception {
        setupServer(null);
        Random r = new Random();
        simpleTest(100, 2, 4096, (out, data) -> {
            try {
                int len = data.length;
                out.write(len >>> 24);
                out.flush();
                Thread.sleep(1);
                out.write((len >>> 16) & 0xFF);
                out.flush();
                Thread.sleep(1);
                out.write((len >>> 8) & 0xFF);
                out.flush();
                Thread.sleep(1);
                out.write(len & 0xFF);
                out.flush();
                Thread.sleep(1);

                for (int i = 0; i < data.length; i++) {
                    out.write(data[i]);
                    if (r.nextDouble() < 0.1) {
                        out.flush();
                        Thread.sleep(1);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}

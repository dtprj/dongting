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
import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.pb.DtFrame;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.github.dtprj.dongting.common.Tick.tick;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author huangli
 */
public class NioServerTest {
    private static final int CMD_IO_PING = 3000;
    private static final int CMD_BIZ_PING1 = 3001;
    private static final int CMD_BIZ_PING2 = 3002;

    private static final int PORT = 9000;

    private NioServer server;

    static class SleepPingProcessor extends NioServer.PingProcessor {

        private final int sleep;

        public SleepPingProcessor(int sleep) {
            this.sleep = sleep;
        }

        @Override
        public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
            if (sleep > 0) {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return super.process(frame, channelContext, reqContext);
        }
    }

    private void setupServer(Consumer<NioServerConfig> consumer) {
        NioServerConfig c = new NioServerConfig();
        c.setPort(PORT);
        if (consumer != null) {
            consumer.accept(c);
        }
        server = new NioServer(c);
        server.register(CMD_IO_PING, new NioServer.PingProcessor(), null);
        server.register(CMD_BIZ_PING1, new NioServer.PingProcessor());
        server.register(CMD_BIZ_PING2, new ReqProcessor() {

            @Override
            public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
                ByteBuffer buf = (ByteBuffer) frame.getBody();
                ByteBufferWriteFrame resp = new ByteBufferWriteFrame(buf);
                resp.setRespCode(CmdCodes.SUCCESS);
                return resp;
            }

            @Override
            public Decoder getDecoder() {
                return new BizByteBufferDecoder();
            }
        });
    }

    @AfterEach
    public void teardown() {
        server.stop();
    }

    @Test
    public void simpleTest() throws Exception {
        setupServer(null);
        server.start();
        Socket s = new Socket("127.0.0.1", PORT);
        try {
            s.setTcpNoDelay(true);
            s.setSoTimeout(30000);
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            simpleTest(CMD_IO_PING, in, out, 1);
            simpleTest(CMD_BIZ_PING1, in, out, 2);
            simpleTest(CMD_BIZ_PING2, in, out, 3);
        } finally {
            DtUtil.close(s);
        }
    }

    private static void simpleTest(int cmd, DataInputStream in, DataOutputStream out, int seq) throws Exception {
        byte[] bs = new byte[new Random().nextInt(3000)];
        DtFrame.Frame reqFrame = DtFrame.Frame.newBuilder().setFrameType(FrameType.TYPE_REQ)
                .setSeq(seq)
                .setCommand(cmd)
                .setBody(ByteString.copyFrom(bs))
                .setTimeout(Duration.ofSeconds(10).toNanos())
                .build();
        byte[] reqFrameBytes = reqFrame.toByteArray();
        out.writeInt(reqFrameBytes.length);
        out.write(reqFrameBytes);
        out.flush();

        int len = in.readInt();
        byte[] resp = new byte[len];
        in.readFully(resp);
        DtFrame.Frame frame = DtFrame.Frame.parseFrom(resp);
        assertEquals(FrameType.TYPE_RESP, frame.getFrameType());
        assertEquals(CmdCodes.SUCCESS, frame.getRespCode());
        assertArrayEquals(bs, frame.getBody().toByteArray());
    }

    @Test
    public void generalTest() throws Exception {
        setupServer(null);
        server.start();
        generalTest(1000, 0, 5000, null);
    }

    private void generalTest(long millis, int innerLoop, int maxBodySize,
                             BiConsumer<DataOutputStream, byte[]> writer) throws Exception {
        Socket s = new Socket("127.0.0.1", PORT);
        try {
            s.setTcpNoDelay(true);
            s.setSoTimeout(30000);
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
                    DtFrame.Frame frame = DtFrame.Frame.newBuilder().setFrameType(FrameType.TYPE_REQ)
                            .setSeq(seq + i)
                            .setCommand(CMD_IO_PING + (i % 3))
                            .setBody(ByteString.copyFrom(map.get(seq + i)))
                            .setTimeout(Duration.ofSeconds(10).toNanos())
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
                    assertEquals(FrameType.TYPE_RESP, frame.getFrameType());
                    assertEquals(CmdCodes.SUCCESS, frame.getRespCode());
                    byte[] expect = map.get(frame.getSeq());
                    if (expect.length != 0) {
                        assertArrayEquals(expect, frame.getBody().toByteArray());
                    } else {
                        assertNotNull(frame.getBody());
                    }
                }
            }
        } finally {
            DtUtil.close(s);
        }
    }

    @Test
    public void multiClientTest() throws Exception {
        setupServer(null);
        server.start();
        final int threads = 100;
        AtomicInteger fail = new AtomicInteger(0);
        Thread[] ts = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            Runnable r = () -> {
                try {
                    generalTest(1000, 0, 5000, null);
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
        server.start();
        generalTest(10, 100, 1024 * 1024, null);
    }

    @Test
    public void clientSlowWriteTest() throws Exception {
        setupServer(null);
        server.start();
        Random r = new Random();
        generalTest(100, 2, 4096, (out, data) -> {
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

                for (byte datum : data) {
                    out.write(datum);
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

    private static int invoke(int seq, int command, int bodySize, DataInputStream in, DataOutputStream out) throws Exception {
        byte[] bs = new byte[bodySize];
        ThreadLocalRandom.current().nextBytes(bs);
        DtFrame.Frame frame = DtFrame.Frame.newBuilder().setFrameType(FrameType.TYPE_REQ)
                .setSeq(seq)
                .setCommand(command)
                .setBody(ByteString.copyFrom(bs))
                .setTimeout(Duration.ofSeconds(tick(1)).toNanos())
                .build();
        byte[] frameBytes = frame.toByteArray();
        out.writeInt(frameBytes.length);
        out.write(frameBytes);
        out.flush();

        int len = in.readInt();
        byte[] resp = new byte[len];
        in.readFully(resp);
        frame = DtFrame.Frame.parseFrom(resp);
        assertEquals(FrameType.TYPE_RESP, frame.getFrameType());
        if (frame.getRespCode() == CmdCodes.SUCCESS) {
            assertArrayEquals(bs, frame.getBody().toByteArray());
        }
        return frame.getRespCode();
    }

    @Test
    public void badCommandTest() throws Exception {
        setupServer(null);
        server.start();
        Socket s = new Socket("127.0.0.1", PORT);
        try {
            s.setTcpNoDelay(true);
            s.setSoTimeout(tick(1000));
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            int code = invoke(1, 12323434, 5000, in, out);
            assertEquals(CmdCodes.COMMAND_NOT_SUPPORT, code);
        } finally {
            DtUtil.close(s);
        }
    }

    private static class InvokeThread extends Thread {
        private final int bodySize;
        AtomicInteger result = new AtomicInteger(-1);

        public InvokeThread(int bodySize) {
            this.bodySize = bodySize;
        }

        @Override
        public void run() {
            try (Socket s = new Socket("127.0.0.1", PORT)) {
                s.setSoTimeout(tick(1000));
                s.setTcpNoDelay(true);
                DataInputStream in = new DataInputStream(s.getInputStream());
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                int code = invoke(1, 50000, bodySize, in, out);
                result.set(code);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void flowControlTest() throws Exception {
        setupServer(c -> {
            c.setMaxInRequests(1);
            c.setBizThreads(1);
            c.setMaxInBytes(10000);
        });
        server.register(50000, new SleepPingProcessor(50));
        server.start();
        {
            InvokeThread t1 = new InvokeThread(100);
            InvokeThread t2 = new InvokeThread(100);
            InvokeThread t3 = new InvokeThread(100);
            t1.start();
            t2.start();
            t3.start();
            t1.join(1000);
            t2.join(1000);
            t3.join(1000);
            assertEquals(CmdCodes.FLOW_CONTROL, t1.result.get() + t2.result.get() + t3.result.get());
        }
        {
            InvokeThread t1 = new InvokeThread(6000);
            InvokeThread t2 = new InvokeThread(6000);
            t1.start();
            t2.start();
            t1.join(1000);
            t2.join(1000);
            assertEquals(CmdCodes.FLOW_CONTROL, t1.result.get() + t2.result.get());
        }
    }

    @Test
    public void badDecoderTest() throws Exception {
        setupServer(null);
        server.register(10001, new ReqProcessor() {
            @Override
            public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
                return null;
            }

            @Override
            public Decoder getDecoder() {
                return new Decoder() {
                    @Override
                    public Object decode(ChannelContext context, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
                        throw new ArrayIndexOutOfBoundsException();
                    }
                };
            }
        });

        server.register(10002, new ReqProcessor() {
            @Override
            public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
                return null;
            }

            @Override
            public Decoder getDecoder() {
                return new Decoder() {
                    @Override
                    public boolean decodeInIoThread() {
                        return false;
                    }

                    @Override
                    public Object decode(ChannelContext context, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
                        throw new ArrayIndexOutOfBoundsException();
                    }
                };
            }
        });
        server.start();
        Socket s = new Socket("127.0.0.1", PORT);
        try {
            s.setTcpNoDelay(true);
            s.setSoTimeout(1000);
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            assertEquals(CmdCodes.SUCCESS, invoke(1, CMD_IO_PING, 5000, in, out));
            assertEquals(CmdCodes.SUCCESS, invoke(2, CMD_BIZ_PING1, 5000, in, out));
            assertEquals(CmdCodes.SUCCESS, invoke(2, CMD_BIZ_PING2, 5000, in, out));

            assertEquals(CmdCodes.BIZ_ERROR, invoke(3, 10001, 5000, in, out));
            assertEquals(CmdCodes.BIZ_ERROR, invoke(4, 10002, 5000, in, out));

            assertEquals(CmdCodes.SUCCESS, invoke(5, CMD_IO_PING, 5000, in, out));
            assertEquals(CmdCodes.SUCCESS, invoke(6, CMD_BIZ_PING1, 5000, in, out));
            assertEquals(CmdCodes.SUCCESS, invoke(7, CMD_BIZ_PING2, 5000, in, out));
        } finally {
            DtUtil.close(s);
        }
    }

    @Test
    public void badProcessorTest() throws Exception {
        setupServer(null);
        server.register(10001, new ReqProcessor() {
            @Override
            public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
                throw new ArrayIndexOutOfBoundsException();
            }

            @Override
            public Decoder getDecoder() {
                return new ByteBufferDecoder();
            }
        }, null);
        server.register(10002, new ReqProcessor() {
            @Override
            public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
                throw new ArrayIndexOutOfBoundsException();
            }

            @Override
            public Decoder getDecoder() {
                return new ByteBufferDecoder();
            }
        });
        server.register(10003, new ReqProcessor() {
            @Override
            public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
                return null;
            }

            @Override
            public Decoder getDecoder() {
                return new ByteBufferDecoder();
            }
        }, null);
        server.register(10004, new ReqProcessor() {
            @Override
            public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
                return null;
            }

            @Override
            public Decoder getDecoder() {
                return new ByteBufferDecoder();
            }
        });
        server.start();

        Socket s = new Socket("127.0.0.1", PORT);
        try {
            s.setTcpNoDelay(true);
            s.setSoTimeout(tick(50));
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            int seq = 0;
            assertEquals(CmdCodes.SUCCESS, invoke(++seq, CMD_IO_PING, 5000, in, out));
            assertEquals(CmdCodes.SUCCESS, invoke(++seq, CMD_BIZ_PING1, 5000, in, out));
            assertEquals(CmdCodes.SUCCESS, invoke(++seq, CMD_BIZ_PING2, 5000, in, out));

            assertEquals(CmdCodes.BIZ_ERROR, invoke(++seq, 10001, 5000, in, out));
            assertEquals(CmdCodes.BIZ_ERROR, invoke(++seq, 10002, 5000, in, out));

            assertEquals(CmdCodes.SUCCESS, invoke(++seq, CMD_IO_PING, 5000, in, out));
            assertEquals(CmdCodes.SUCCESS, invoke(++seq, CMD_BIZ_PING1, 5000, in, out));
            assertEquals(CmdCodes.SUCCESS, invoke(++seq, CMD_BIZ_PING2, 5000, in, out));

            assertThrows(SocketTimeoutException.class, () -> invoke(123456, 10003, 5000, in, out));
        } finally {
            DtUtil.close(s);
        }
        s = new Socket("127.0.0.1", PORT);
        try {
            s.setTcpNoDelay(true);
            s.setSoTimeout(tick(50));
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            assertThrows(SocketTimeoutException.class, () -> invoke(10003, 10004, 5000, in, out));
        } finally {
            DtUtil.close(s);
        }
        s = new Socket("127.0.0.1", PORT);
        try {
            s.setTcpNoDelay(true);
            s.setSoTimeout(tick(50));
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            assertThrows(SocketTimeoutException.class, () -> invoke(10004, 10004, 5000, in, out));
        } finally {
            DtUtil.close(s);
        }
    }

    private ReqProcessor threadNotMatchProcessor() {
        return new NioServer.PingProcessor() {
            @Override
            public Decoder getDecoder() {
                return new Decoder() {
                    @Override
                    public boolean decodeInIoThread() {
                        return false;
                    }

                    @Override
                    public Object decode(ChannelContext context, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
                        return null;
                    }
                };
            }
        };
    }

    @Test
    public void testThreadNotMatch() {
        // processor run in io thread, but decoder.decodeInIoThread() == false
        setupServer(null);
        ReqProcessor badProcessor = threadNotMatchProcessor();
        assertThrows(DtException.class, () -> server.register(10005, badProcessor, null));
        server.start();
    }

    @Test
    public void testThreadNotMatch2() {
        // processor run in io thread, but decoder.decodeInIoThread() == false
        setupServer(c -> c.setBizThreads(0));
        ReqProcessor badProcessor = threadNotMatchProcessor();
        server.register(10005, badProcessor);
        assertThrows(DtException.class, () -> server.start());
    }

    @Test
    public void testRegisterAfterStart() {
        setupServer(null);
        server.start();
        assertThrows(DtException.class, () -> server.register(12345, new NioServer.PingProcessor()));
        assertThrows(DtException.class, () -> server.register(12345, new NioServer.PingProcessor(), null));
    }

    @Test
    public void asyncProcessorTest() throws Exception {
        setupServer(null);
        server.register(3333, new ReqProcessor() {
            @Override
            public WriteFrame process(ReadFrame frame, ChannelContext channelContext, ReqContext reqContext) {
                Thread t = new Thread(() -> {
                    RefBufWriteFrame resp = new RefBufWriteFrame((RefBuffer) frame.getBody());
                    resp.setRespCode(CmdCodes.SUCCESS);
                    channelContext.getRespWriter().writeRespInBizThreads(frame, resp, new DtTime(1, TimeUnit.SECONDS));
                });
                t.start();
                return null;
            }

            @Override
            public Decoder getDecoder() {
                return new ByteBufferDecoder();
            }
        });
        server.start();

        Socket s = new Socket("127.0.0.1", PORT);
        try {
            s.setTcpNoDelay(true);
            s.setSoTimeout(500);
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            assertEquals(CmdCodes.SUCCESS, invoke(1, 3333, 5000, in, out));
        } finally {
            DtUtil.close(s);
        }
    }
}

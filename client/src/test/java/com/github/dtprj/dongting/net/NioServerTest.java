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

import com.github.dtprj.dongting.common.DtTime;
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
        Socket s = new Socket("127.0.0.1", PORT);
        DataInputStream in = new DataInputStream(s.getInputStream());
        DataOutputStream out = new DataOutputStream(s.getOutputStream());
        Random r = new Random();
        DtTime t = new DtTime();
        int seq = 0;
        while (t.elapse(TimeUnit.MILLISECONDS) < 1000) {
            int count = r.nextInt(10) + 1;
            HashMap<Integer, byte[]> map = new HashMap<>();
            for (int i = 0; i < count; i++) {
                if (r.nextDouble() < 0.05) {
                    //empty test
                    map.put(seq + i, new byte[]{});
                } else {
                    byte[] bs = new byte[r.nextInt(5000)];
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
                out.writeInt(bs.length);
                out.write(bs);
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
    }
}

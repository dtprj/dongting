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

import com.github.dtprj.dongting.pb.DtFrame;
import com.google.protobuf.ByteString;

import java.io.DataOutputStream;
import java.net.Socket;

public class NioServerTest {
    public static void main(String[] args) throws Exception {
        NioServerConfig c = new NioServerConfig();
        c.setPort(9000);
        NioServer server = new NioServer(c);
        server.register(Commands.CMD_PING, f-> System.out.println(f.getCommand()));

        server.start();

        Thread.sleep(100);
        Socket s = new Socket("127.0.0.1", 9000);
        byte[] bs = DtFrame.Frame.newBuilder()
                .setCommand(Commands.CMD_PING)
                .setBody(ByteString.copyFrom("hello".getBytes()))
                .build().toByteArray();
        DataOutputStream os = new DataOutputStream(s.getOutputStream());
        os.writeInt(bs.length);
        os.write(bs);
        os.flush();
        s.close();
    }
}

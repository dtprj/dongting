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

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

/**
 * @author huangli
 */
public class ChannelContext {
    private SocketChannel channel;
    private SocketAddress remoteAddr;
    private SocketAddress localAddr;
    private RespWriter respWriter;

    public SocketChannel getChannel() {
        return channel;
    }

    protected void setChannel(SocketChannel channel) {
        this.channel = channel;
    }

    public SocketAddress getRemoteAddr() {
        return remoteAddr;
    }

    protected void setRemoteAddr(SocketAddress remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public SocketAddress getLocalAddr() {
        return localAddr;
    }

    protected void setLocalAddr(SocketAddress localAddr) {
        this.localAddr = localAddr;
    }

    public RespWriter getRespWriter() {
        return respWriter;
    }

    protected void setRespWriter(RespWriter respWriter) {
        this.respWriter = respWriter;
    }

}

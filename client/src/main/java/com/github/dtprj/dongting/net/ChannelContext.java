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
    private final DtChannelImpl dtChannel;
    private final SocketChannel channel;
    private final SocketAddress remoteAddr;
    private final SocketAddress localAddr;
    private final RespWriter respWriter;

    ChannelContext(DtChannelImpl dtChannel, SocketChannel channel, SocketAddress remoteAddr, SocketAddress localAddr, RespWriter respWriter) {
        this.dtChannel = dtChannel;
        this.channel = channel;
        this.remoteAddr = remoteAddr;
        this.localAddr = localAddr;
        this.respWriter = respWriter;
    }

    public DtChannel getDtChannel() {
        return dtChannel;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public SocketAddress getRemoteAddr() {
        return remoteAddr;
    }

    public SocketAddress getLocalAddr() {
        return localAddr;
    }

    public RespWriter getRespWriter() {
        return respWriter;
    }

}

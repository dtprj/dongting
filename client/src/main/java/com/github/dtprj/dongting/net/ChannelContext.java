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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.pb.PbParser;

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

    // this decoder can only be used in io thread
    private StringFieldDecoder ioThreadStrDecoder;
    private Object ioDecodeStatus;
    private ByteBufferPool ioHeapBufferPool;
    private PbParser ioParser;

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

    /**
     * this decoder can only be used in io thread
     */
    public StringFieldDecoder getIoThreadStrDecoder() {
        return ioThreadStrDecoder;
    }

    public void setIoThreadStrDecoder(StringFieldDecoder ioThreadStrDecoder) {
        this.ioThreadStrDecoder = ioThreadStrDecoder;
    }

    public Object getIoDecodeStatus() {
        return ioDecodeStatus;
    }

    public void setIoDecodeStatus(Object ioDecodeStatus) {
        this.ioDecodeStatus = ioDecodeStatus;
    }

    public ByteBufferPool getIoHeapBufferPool() {
        return ioHeapBufferPool;
    }

    public void setIoHeapBufferPool(ByteBufferPool ioHeapBufferPool) {
        this.ioHeapBufferPool = ioHeapBufferPool;
    }

    public RespWriter getRespWriter() {
        return respWriter;
    }

    protected void setRespWriter(RespWriter respWriter) {
        this.respWriter = respWriter;
    }

    public PbParser getIoParser() {
        return ioParser;
    }

    public void setIoParser(PbParser ioParser) {
        this.ioParser = ioParser;
    }
}

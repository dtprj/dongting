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
package com.github.dtprj.dongting.dtkv;

import com.github.dtprj.dongting.codec.ByteArrayDecoder;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.PbNoCopyDecoder;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.SmallNoCopyWritePacket;
import com.github.dtprj.dongting.raft.RaftClient;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
@SuppressWarnings("Convert2Diamond")
public class KvClient extends AbstractLifeCircle {
    private final RaftClient raftClient;

    public KvClient(NioClientConfig nioClientConfig) {
        this.raftClient = new RaftClient(nioClientConfig);
    }

    public CompletableFuture<Void> put(int groupId, String key, byte[] value, DtTime timeout) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        SmallNoCopyWritePacket wf = new SmallNoCopyWritePacket() {

            private final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            @Override
            protected void encodeBody(ByteBuffer buf) {
                PbUtil.writeUnsignedInt32(buf, 1, groupId);
                PbUtil.writeBytes(buf, 2, keyBytes);
                PbUtil.writeBytes(buf, 3, value);
            }

            @Override
            protected int calcActualBodySize() {
                return PbUtil.accurateUnsignedIntSize(1, groupId)
                        + PbUtil.accurateLengthDelimitedSize(2, keyBytes.length)
                        + PbUtil.accurateLengthDelimitedSize(3, value.length);
            }
        };
        wf.setCommand(Commands.DTKV_PUT);
        CompletableFuture<Void> f = new CompletableFuture<>();
        raftClient.sendRequest(groupId, wf, Decoder.VOID_DECODER, timeout, RpcCallback.create(f));
        return f;
    }

    public CompletableFuture<byte[]> get(int groupId, String key, DtTime timeout) {
        Objects.requireNonNull(key);
        SmallNoCopyWritePacket wf = new SmallNoCopyWritePacket() {

            private final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            @Override
            protected void encodeBody(ByteBuffer buf) {
                PbUtil.writeUnsignedInt32(buf, 1, groupId);
                PbUtil.writeBytes(buf, 2, keyBytes);
            }

            @Override
            protected int calcActualBodySize() {
                return PbUtil.accurateUnsignedIntSize(1, groupId)
                        + PbUtil.accurateLengthDelimitedSize(2, keyBytes.length);
            }
        };
        wf.setCommand(Commands.DTKV_GET);
        CompletableFuture<byte[]> f = new CompletableFuture<>();
        raftClient.sendRequest(groupId, wf, ByteArrayDecoder.INSTANCE, timeout, RpcCallback.create(f));
        return f;
    }

    public CompletableFuture<Boolean> remove(int groupId, String key, DtTime timeout) {
        Objects.requireNonNull(key);
        SmallNoCopyWritePacket wf = new SmallNoCopyWritePacket() {

            private final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            @Override
            protected void encodeBody(ByteBuffer buf) {
                PbUtil.writeUnsignedInt32(buf, 1, groupId);
                PbUtil.writeBytes(buf, 2, keyBytes);
            }

            @Override
            protected int calcActualBodySize() {
                return PbUtil.accurateUnsignedIntSize(1, groupId)
                        + PbUtil.accurateLengthDelimitedSize(2, keyBytes.length);
            }
        };
        wf.setCommand(Commands.DTKV_REMOVE);
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        RpcCallback<Integer> c = new RpcCallback<Integer>() {
            @Override
            public void success(ReadPacket<Integer> resp) {
                f.complete(resp.getBody() != null && resp.getBody() != 0);
            }

            @Override
            public void fail(Throwable ex) {
                f.completeExceptionally(ex);
            }
        };
        raftClient.sendRequest(groupId, wf, PbNoCopyDecoder.SIMPLE_INT_DECODER, timeout, c);
        return f;
    }

    @Override
    protected void doStart() {
        raftClient.start();
    }

    protected void doStop(DtTime timeout, boolean force) {
        raftClient.stop(timeout);
    }

    public RaftClient getRaftClient() {
        return raftClient;
    }
}

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

import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.NioClientConfig;
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

    public CompletableFuture<Integer> put(int groupId, String key, byte[] value, DtTime timeout) {
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
        CompletableFuture<Integer> f = new CompletableFuture<>();
        raftClient.sendRequest(groupId, wf, c -> DecoderCallback.VOID_DECODE_CALLBACK,
                timeout, RpcCallback.createBizCodeCallback(f));
        return f;
    }

    public CompletableFuture<KvNode> get(int groupId, String key, DtTime timeout) {
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
        CompletableFuture<KvNode> f = new CompletableFuture<>();

        raftClient.sendRequest(groupId, wf, ctx -> ctx.toDecoderCallback(new KvNode.Callback()),
                timeout, RpcCallback.create(f));
        return f;
    }

    public CompletableFuture<Integer> remove(int groupId, String key, DtTime timeout) {
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
        CompletableFuture<Integer> f = new CompletableFuture<>();
        raftClient.sendRequest(groupId, wf, c -> DecoderCallback.VOID_DECODE_CALLBACK,
                timeout, RpcCallback.createBizCodeCallback(f));
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

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
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.SmallNoCopyWriteFrame;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.RaftClient;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class KvClient extends AbstractLifeCircle {
    private final RaftClient raftClient;

    public KvClient(NioClientConfig nioClientConfig) {
        this.raftClient = new RaftClient(nioClientConfig);
    }

    public CompletableFuture<Void> put(int groupId, String key, byte[] value, DtTime timeout) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        WriteFrame wf = new SmallNoCopyWriteFrame() {

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
        return raftClient.sendRequest(groupId, wf, Decoder.VOID_DECODER, timeout)
                .thenApply(r -> null);
    }

    public CompletableFuture<byte[]> get(int groupId, String key, DtTime timeout) {
        Objects.requireNonNull(key);
        WriteFrame wf = new SmallNoCopyWriteFrame() {

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
        return raftClient.sendRequest(groupId, wf, ByteArrayDecoder.INSTANCE, timeout)
                .thenApply(ReadFrame::getBody);
    }

    public CompletableFuture<Boolean> remove(int groupId, String key, DtTime timeout) {
        Objects.requireNonNull(key);
        WriteFrame wf = new SmallNoCopyWriteFrame() {

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
        return raftClient.sendRequest(groupId, wf, PbNoCopyDecoder.SIMPLE_INT_DECODER, timeout)
                .thenApply(f -> f.getBody() != null && f.getBody() != 0);
    }

    @Override
    protected void doStart() {
        raftClient.start();
    }

    protected void doStop(boolean force) {
        raftClient.stop();
    }
}

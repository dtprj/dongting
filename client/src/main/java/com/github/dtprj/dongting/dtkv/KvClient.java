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

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.NetBizCodeException;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.raft.RaftClient;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftTimeoutException;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author huangli
 */
public class KvClient extends AbstractLifeCircle {
    private final RaftClient raftClient;

    private static final DecoderCallbackCreator<KvResp> DECODER = ctx -> ctx.toDecoderCallback(new KvResp.Callback());

    public KvClient() {
        this(new NioClientConfig());
    }

    public KvClient(NioClientConfig nioConfig) {
        this.raftClient = new RaftClient(nioConfig);
    }

    private static RpcCallback<KvResp> raftIndexCallback(FutureCallback<Long> c, int anotherSuccessCode) {
        return (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(c, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.CODE_SUCCESS || bc == anotherSuccessCode) {
                    FutureCallback.callSuccess(c, result.getBody().raftIndex);
                } else {
                    FutureCallback.callFail(c, new NetBizCodeException(bc, KvCodes.toStr(result.getBizCode())));
                }
            }
        };
    }

    private <T> T waitFuture(CompletableFuture<T> f, DtTime timeout) {
        try {
            return f.get(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            DtUtil.restoreInterruptStatus();
            throw new RaftException("interrupted", e);
        } catch (ExecutionException e) {
            throw new RaftException("execution exception: " + e.getCause().getMessage(), e.getCause());
        } catch (TimeoutException e) {
            throw new RaftTimeoutException("timeout: " + timeout.getTimeout(TimeUnit.MILLISECONDS) + "ms", e);
        }
    }

    public long put(int groupId, byte[] key, byte[] value) {
        CompletableFuture<Long> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        put(groupId, key, value, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void put(int groupId, byte[] key, byte[] value, FutureCallback<Long> callback) {
        put(groupId, key, value, raftClient.createDefaultTimeout(), callback);
    }

    private void put(int groupId, byte[] key, byte[] value, DtTime timeout, FutureCallback<Long> callback) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        KvReq r = new KvReq(groupId, key, value);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_PUT);
        RpcCallback<KvResp> c = raftIndexCallback(callback, KvCodes.CODE_SUCCESS_OVERWRITE);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, c);
    }

    public KvNode get(int groupId, byte[] key) {
        CompletableFuture<KvNode> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        get(groupId, key, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void get(int groupId, byte[] key, FutureCallback<KvNode> callback) {
        get(groupId, key, raftClient.createDefaultTimeout(), callback);
    }

    private void get(int groupId, byte[] key, DtTime timeout, FutureCallback<KvNode> callback) {
        Objects.requireNonNull(key);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_GET);

        raftClient.sendRequest(groupId, wf, DECODER, timeout, (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.CODE_SUCCESS || bc == KvCodes.CODE_NOT_FOUND) {
                    KvResp resp = result.getBody();
                    KvNode n = (resp == null || resp.results == null || resp.results.isEmpty()) ? null : resp.results.get(0).getNode();
                    FutureCallback.callSuccess(callback, n);
                } else {
                    FutureCallback.callFail(callback, new NetBizCodeException(bc, KvCodes.toStr(result.getBizCode())));
                }
            }
        });
    }

    public List<KvResult> list(int groupId, byte[] key) {
        CompletableFuture<List<KvResult>> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        list(groupId, key, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void list(int groupId, byte[] key, FutureCallback<List<KvResult>> callback) {
        list(groupId, key, raftClient.createDefaultTimeout(), callback);
    }

    private void list(int groupId, byte[] key, DtTime timeout, FutureCallback<List<KvResult>> callback) {
        Objects.requireNonNull(key);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_LIST);

        raftClient.sendRequest(groupId, wf, DECODER, timeout, (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.CODE_SUCCESS) {
                    KvResp resp = result.getBody();
                    FutureCallback.callSuccess(callback, (resp == null || resp.results == null)
                            ? Collections.emptyList() : resp.results);
                } else {
                    FutureCallback.callFail(callback, new NetBizCodeException(bc, KvCodes.toStr(result.getBizCode())));
                }
            }
        });
    }

    public long remove(int groupId, byte[] key) {
        CompletableFuture<Long> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        remove(groupId, key, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void remove(int groupId, byte[] key, FutureCallback<Long> callback) {
        remove(groupId, key, raftClient.createDefaultTimeout(), callback);
    }

    private void remove(int groupId, byte[] key, DtTime timeout, FutureCallback<Long> callback) {
        Objects.requireNonNull(key);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_REMOVE);
        RpcCallback<KvResp> c = raftIndexCallback(callback, KvCodes.CODE_NOT_FOUND);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, c);
    }

    public long mkdir(int groupId, byte[] key) {
        CompletableFuture<Long> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        mkdir(groupId, key, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void mkdir(int groupId, byte[] key, FutureCallback<Long> callback) {
        mkdir(groupId, key, raftClient.createDefaultTimeout(), callback);
    }

    private void mkdir(int groupId, byte[] key, DtTime timeout, FutureCallback<Long> callback) {
        Objects.requireNonNull(key);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_MKDIR);
        RpcCallback<KvResp> c = raftIndexCallback(callback, KvCodes.CODE_DIR_EXISTS);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, c);
    }

    public KvResp batchPut(int groupId, List<byte[]> keys, List<byte[]> values) {
        CompletableFuture<KvResp> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        batchPut(groupId, keys, values, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void batchPut(int groupId, List<byte[]> keys, List<byte[]> values, FutureCallback<KvResp> callback) {
        batchPut(groupId, keys, values, raftClient.createDefaultTimeout(), callback);
    }

    private void batchPut(int groupId, List<byte[]> keys, List<byte[]> values,
                          DtTime timeout, FutureCallback<KvResp> callback) {
        Objects.requireNonNull(keys);
        Objects.requireNonNull(values);
        if (keys.isEmpty() || keys.size() != values.size()) {
            throw new IllegalArgumentException("keys and values must be same size and not empty");
        }
        KvReq r = new KvReq(groupId, keys, values);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_BATCH_PUT);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, kvRespCallback(callback));
    }

    private static RpcCallback<KvResp> kvRespCallback(FutureCallback<KvResp> callback) {
        return (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.CODE_SUCCESS) {
                    KvResp resp = result.getBody();
                    FutureCallback.callSuccess(callback, resp);
                } else {
                    FutureCallback.callFail(callback, new NetBizCodeException(bc, KvCodes.toStr(result.getBizCode())));
                }
            }
        };
    }

    public List<KvNode> batchGet(int groupId, List<byte[]> keys) {
        CompletableFuture<List<KvNode>> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        batchGet(groupId, keys, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void batchGet(int groupId, List<byte[]> keys, FutureCallback<List<KvNode>> callback) {
        batchGet(groupId, keys, raftClient.createDefaultTimeout(), callback);
    }

    private void batchGet(int groupId, List<byte[]> keys, DtTime timeout, FutureCallback<List<KvNode>> callback) {
        Objects.requireNonNull(keys);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("keys must not be empty");
        }
        KvReq r = new KvReq(groupId, keys, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_BATCH_GET);

        raftClient.sendRequest(groupId, wf, DECODER, timeout, (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.CODE_SUCCESS) {
                    KvResp resp = result.getBody();
                    if (resp == null || resp.results == null) {
                        FutureCallback.callSuccess(callback, Collections.emptyList());
                    } else {
                        List<KvNode> nodes = resp.results.stream().map(KvResult::getNode).collect(Collectors.toList());
                        FutureCallback.callSuccess(callback, nodes);
                    }
                } else {
                    FutureCallback.callFail(callback, new NetBizCodeException(bc, KvCodes.toStr(result.getBizCode())));
                }
            }
        });
    }

    public KvResp batchRemove(int groupId, List<byte[]> keys) {
        CompletableFuture<KvResp> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        batchRemove(groupId, keys, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void batchRemove(int groupId, List<byte[]> keys, FutureCallback<KvResp> callback) {
        batchRemove(groupId, keys, raftClient.createDefaultTimeout(), callback);
    }

    private void batchRemove(int groupId, List<byte[]> keys, DtTime timeout, FutureCallback<KvResp> callback) {
        Objects.requireNonNull(keys);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("keys must not be empty");
        }
        KvReq r = new KvReq(groupId, keys, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_BATCH_REMOVE);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, kvRespCallback(callback));
    }

    public long compareAndSet(int groupId, byte[] key, byte[] expectValue, byte[] newValue) {
        CompletableFuture<Long> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        compareAndSet(groupId, key, expectValue, newValue, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void compareAndSet(int groupId, byte[] key, byte[] expectValue, byte[] newValue, FutureCallback<Long> callback) {
        compareAndSet(groupId, key, expectValue, newValue, raftClient.createDefaultTimeout(), callback);
    }

    private void compareAndSet(int groupId, byte[] key, byte[] expectValue, byte[] newValue,
                               DtTime timeout, FutureCallback<Long> callback) {
        Objects.requireNonNull(key);
        KvReq r = new KvReq(groupId, key, newValue, null, null, expectValue);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_CAS);
        DecoderCallbackCreator<KvResp> dc = ctx -> ctx.toDecoderCallback(new KvResp.Callback());
        raftClient.sendRequest(groupId, wf, dc, timeout, (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.CODE_SUCCESS) {
                    FutureCallback.callSuccess(callback, result.getBody().raftIndex);
                } else if (bc == KvCodes.CODE_CAS_MISMATCH) {
                    FutureCallback.callSuccess(callback, 0L);
                } else {
                    FutureCallback.callFail(callback, new NetBizCodeException(bc, KvCodes.toStr(bc)));
                }
            }
        });
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

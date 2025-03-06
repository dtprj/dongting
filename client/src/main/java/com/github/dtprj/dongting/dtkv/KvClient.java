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

    public KvClient() {
        this(new NioClientConfig());
    }

    public KvClient(NioClientConfig nioConfig) {
        this.raftClient = new RaftClient(nioConfig);
    }

    private static RpcCallback<Void> voidCallback(FutureCallback<Void> c, int anotherSuccessCode) {
        return (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(c, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.CODE_SUCCESS || bc == anotherSuccessCode) {
                    FutureCallback.callSuccess(c, null);
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
        } catch (Exception e) {
            throw new RaftException("execution exception: " + e.getMessage(), e);
        }
    }

    public void put(int groupId, byte[] key, byte[] value, DtTime timeout) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        put(groupId, key, value, timeout, FutureCallback.fromFuture(f));
        waitFuture(f, timeout);
    }

    public void put(int groupId, byte[] key, byte[] value, DtTime timeout, FutureCallback<Void> callback) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        KvReq r = new KvReq(groupId, key, value);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_PUT);
        RpcCallback<Void> c = voidCallback(callback, KvCodes.CODE_SUCCESS_OVERWRITE);
        raftClient.sendRequest(groupId, wf, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR, timeout, c);
    }

    public KvNode get(int groupId, byte[] key, DtTime timeout) {
        CompletableFuture<KvNode> f = new CompletableFuture<>();
        get(groupId, key, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void get(int groupId, byte[] key, DtTime timeout, FutureCallback<KvNode> callback) {
        Objects.requireNonNull(key);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_GET);

        DecoderCallbackCreator<KvResp> dc = ctx -> ctx.toDecoderCallback(new KvResp.Callback());
        raftClient.sendRequest(groupId, wf, dc, timeout, (result, ex) -> {
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

    public List<KvResult> list(int groupId, byte[] key, DtTime timeout) {
        CompletableFuture<List<KvResult>> f = new CompletableFuture<>();
        list(groupId, key, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void list(int groupId, byte[] key, DtTime timeout, FutureCallback<List<KvResult>> callback) {
        Objects.requireNonNull(key);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_LIST);

        DecoderCallbackCreator<KvResp> dc = ctx -> ctx.toDecoderCallback(new KvResp.Callback());
        raftClient.sendRequest(groupId, wf, dc, timeout, (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.CODE_SUCCESS) {
                    KvResp resp = result.getBody();
                    FutureCallback.callSuccess(callback, resp == null ? null : resp.results);
                } else {
                    FutureCallback.callFail(callback, new NetBizCodeException(bc, KvCodes.toStr(result.getBizCode())));
                }
            }
        });
    }

    public void remove(int groupId, byte[] key, DtTime timeout) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        remove(groupId, key, timeout, FutureCallback.fromFuture(f));
        waitFuture(f, timeout);
    }

    public void remove(int groupId, byte[] key, DtTime timeout, FutureCallback<Void> callback) {
        Objects.requireNonNull(key);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_REMOVE);
        RpcCallback<Void> c = voidCallback(callback, KvCodes.CODE_NOT_FOUND);
        raftClient.sendRequest(groupId, wf, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR, timeout, c);
    }

    public void mkdir(int groupId, byte[] key, DtTime timeout) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        mkdir(groupId, key, timeout, FutureCallback.fromFuture(f));
        waitFuture(f, timeout);
    }

    public void mkdir(int groupId, byte[] key, DtTime timeout, FutureCallback<Void> callback) {
        Objects.requireNonNull(key);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_MKDIR);
        RpcCallback<Void> c = voidCallback(callback, KvCodes.CODE_DIR_EXISTS);
        raftClient.sendRequest(groupId, wf, DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR, timeout, c);
    }

    public int[] batchPut(int groupId, List<byte[]> keys, List<byte[]> values, DtTime timeout) {
        CompletableFuture<int[]> f = new CompletableFuture<>();
        batchPut(groupId, keys, values, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void batchPut(int groupId, List<byte[]> keys, List<byte[]> values,
                         DtTime timeout, FutureCallback<int[]> callback) {
        Objects.requireNonNull(keys);
        Objects.requireNonNull(values);
        if (keys.isEmpty() || keys.size() != values.size()) {
            throw new IllegalArgumentException("keys and values must be same size and not empty");
        }
        KvReq r = new KvReq(groupId, keys, values);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_BATCH_PUT);
        DecoderCallbackCreator<KvResp> dc = ctx -> ctx.toDecoderCallback(new KvResp.Callback());
        raftClient.sendRequest(groupId, wf, dc, timeout, batchCodesCallback(callback));
    }

    private static RpcCallback<KvResp> batchCodesCallback(FutureCallback<int[]> callback) {
        return (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else{
                int bc = result.getBizCode();
                if (bc == KvCodes.CODE_SUCCESS) {
                    KvResp resp = result.getBody();
                    FutureCallback.callSuccess(callback, resp == null ? null : resp.codes);
                } else {
                    FutureCallback.callFail(callback, new NetBizCodeException(bc, KvCodes.toStr(result.getBizCode())));
                }
            }
        };
    }

    public List<KvNode> batchGet(int groupId, List<byte[]> keys, DtTime timeout) {
        CompletableFuture<List<KvNode>> f = new CompletableFuture<>();
        batchGet(groupId, keys, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void batchGet(int groupId, List<byte[]> keys, DtTime timeout,
                         FutureCallback<List<KvNode>> callback) {
        Objects.requireNonNull(keys);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("keys must not be empty");
        }
        KvReq r = new KvReq(groupId, keys, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_BATCH_GET);

        DecoderCallbackCreator<KvResp> dc = ctx -> ctx.toDecoderCallback(new KvResp.Callback());
        raftClient.sendRequest(groupId, wf, dc, timeout, (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else{
                int bc = result.getBizCode();
                if (bc == KvCodes.CODE_SUCCESS) {
                    KvResp resp = result.getBody();
                    List<KvNode> nodes = (resp == null || resp.results == null) ? null : resp.results.stream().map(
                            KvResult::getNode).collect(Collectors.toList());
                    FutureCallback.callSuccess(callback, nodes);
                } else {
                    FutureCallback.callFail(callback, new NetBizCodeException(bc, KvCodes.toStr(result.getBizCode())));
                }
            }
        });
    }

    public int[] batchRemove(int groupId, List<byte[]> keys, DtTime timeout) {
        CompletableFuture<int[]> f = new CompletableFuture<>();
        batchRemove(groupId, keys, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void batchRemove(int groupId, List<byte[]> keys, DtTime timeout, FutureCallback<int[]> callback) {
        Objects.requireNonNull(keys);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("keys must not be empty");
        }
        KvReq r = new KvReq(groupId, keys, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_BATCH_REMOVE);
        DecoderCallbackCreator<KvResp> dc = ctx -> ctx.toDecoderCallback(new KvResp.Callback());
        raftClient.sendRequest(groupId, wf, dc, timeout, batchCodesCallback(callback));
    }

    public boolean compareAndSet(int groupId, byte[] key, byte[] expectValue, byte[] newValue, DtTime timeout) {
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        compareAndSet(groupId, key, expectValue, newValue, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    public void compareAndSet(int groupId, byte[] key, byte[] expectValue, byte[] newValue,
                              DtTime timeout, FutureCallback<Boolean> callback) {
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
                    FutureCallback.callSuccess(callback, true);
                } else if (bc == KvCodes.CODE_CAS_MISMATCH) {
                    FutureCallback.callSuccess(callback, false);
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

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
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.NetBizCodeException;
import com.github.dtprj.dongting.net.NetException;
import com.github.dtprj.dongting.net.NetTimeoutException;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.raft.RaftClient;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * DtKv client, all read operations are lease-read and still linearizable.
 *
 * @author huangli
 */
public class KvClient extends AbstractLifeCircle {
    private final RaftClient raftClient;
    private final ClientWatchManager clientWatchManager;

    public static final byte SEPARATOR = '.';
    public static final int MAX_KEY_SIZE = 8 * 1024;
    public static final int MAX_VALUE_SIZE = 1024 * 1024;

    private static final DecoderCallbackCreator<KvResp> DECODER = ctx -> ctx.toDecoderCallback(new KvResp.Callback());

    public KvClient() {
        this(new NioClientConfig());
    }

    public KvClient(NioClientConfig nioConfig) {
        this.raftClient = new RaftClient(nioConfig);
        this.clientWatchManager = createClientWatchManager();
        raftClient.getNioClient().register(Commands.DTKV_WATCH_NOTIFY_PUSH, new WatchProcessor(clientWatchManager));
    }

    protected ClientWatchManager createClientWatchManager() {
        return new ClientWatchManager(this, () -> getStatus() >= STATUS_PREPARE_STOP, 60_000);
    }

    private static RpcCallback<KvResp> raftIndexCallback(FutureCallback<Long> c, int anotherSuccessCode) {
        return (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(c, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.SUCCESS || bc == anotherSuccessCode) {
                    FutureCallback.callSuccess(c, result.getBody().raftIndex);
                } else {
                    FutureCallback.callFail(c, new KvException(bc));
                }
            }
        };
    }

    private <T> T waitFuture(CompletableFuture<T> f, DtTime timeout) {
        try {
            return f.get(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            DtUtil.restoreInterruptStatus();
            throw new NetException("interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof KvException) {
                throw (KvException) cause;
            }
            throw new NetException(e);
        } catch (TimeoutException e) {
            throw new NetTimeoutException("timeout: " + timeout.getTimeout(TimeUnit.MILLISECONDS) + "ms", e);
        }
    }

    private void notNullOrEmpty(byte[] key, String fieldName) {
        Objects.requireNonNull(key, fieldName + " must not be null");
        if (key.length == 0) {
            throw new IllegalArgumentException(fieldName + " must not be empty");
        }
    }

    public static int checkKey(byte[] bs, int maxKeySize, boolean allowEmpty, boolean fullCheck) {
        if (bs == null || bs.length == 0) {
            if (allowEmpty) {
                return KvCodes.SUCCESS;
            } else {
                return KvCodes.INVALID_KEY;
            }
        }
        if (bs.length > maxKeySize) {
            return KvCodes.KEY_TOO_LONG;
        }
        if (bs[0] == SEPARATOR || bs[bs.length - 1] == SEPARATOR) {
            return KvCodes.INVALID_KEY;
        }
        if (fullCheck) {
            int lastSep = -1;
            for (int len = bs.length, i = 0; i < len; i++) {
                if (bs[i] == ' ') {
                    return KvCodes.INVALID_KEY;
                }
                if (bs[i] == SEPARATOR) {
                    if (lastSep == i - 1) {
                        return KvCodes.INVALID_KEY;
                    }
                    lastSep = i;
                }
            }
        }
        return KvCodes.SUCCESS;
    }

    private void checkKey(byte[] key, boolean allowEmpty) {
        int c = checkKey(key, MAX_KEY_SIZE, allowEmpty, true);
        if (c != KvCodes.SUCCESS) {
            throw new IllegalArgumentException("invalid key: " + KvCodes.toStr(c));
        }
    }

    /**
     * Synchronously put a key-value pair into the kv store.
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @param value not null or empty
     * @return raft index of this write operation, it is useless in most cases.
     * @throws KvException If K/V node already exists without a tll, throws KvException with code IS_TEMP_NODE;
     * @throws com.github.dtprj.dongting.net.NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public long put(int groupId, byte[] key, byte[] value) throws KvException, NetException {
        CompletableFuture<Long> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        put(groupId, key, value, -1, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    /**
     * Asynchronously put a key-value pair into the kv store.
     * If K/V node already exists without a tll, the callback will complete with a KvException with code IS_TEMP_NODE.
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @param value not null or empty
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     */
    public void put(int groupId, byte[] key, byte[] value, FutureCallback<Long> callback) {
        put(groupId, key, value, -1, raftClient.createDefaultTimeout(), callback);
    }

    /**
     * Synchronously put a key-value pair into the kv store, with a ttl.
     * If K/V node already exists with a tll, it will be overwritten and ttl will be updated.
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @param value not null or empty
     * @return raft index of this write operation, it is useless in most cases.
     * @throws KvException If K/V node already exists without a tll, throws KvException with code NOT_TEMP_NODE;
     *                     If try to update K/V node created by another client, throws KvException with code NOT_OWNER.
     * @throws com.github.dtprj.dongting.net.NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public long putTemp(int groupId, byte[] key, byte[] value, long ttlMillis) throws KvException, NetException {
        DtUtil.checkPositive(ttlMillis, "ttlMillis");
        CompletableFuture<Long> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        put(groupId, key, value, ttlMillis, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    /**
     * Asynchronously put a key-value pair into the kv store, with a ttl.
     * If K/V node already exists with a tll, it will be overwritten and ttl will be updated.
     * If K/V node already exists without a tll, the callback will complete with a KvException with code NOT_TEMP_NODE.
     * If try to update K/V node created by another client, the callback will complete with a KvException with code NOT_OWNER.
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @param value not null or empty
     * @param ttlMillis time to live in milliseconds, must be positive
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     */
    public void putTemp(int groupId, byte[] key, byte[] value, long ttlMillis, FutureCallback<Long> callback) {
        DtUtil.checkPositive(ttlMillis, "ttlMillis");
        put(groupId, key, value, ttlMillis, raftClient.createDefaultTimeout(), callback);
    }

    private void put(int groupId, byte[] key, byte[] value, long ttlMillis, DtTime timeout,
                     FutureCallback<Long> callback) {
        checkKey(key, false);
        notNullOrEmpty(value, "value");
        KvReq r = new KvReq(groupId, key, value);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(ttlMillis > 0 ? Commands.DTKV_PUT_TEMP_NODE : Commands.DTKV_PUT);
        RpcCallback<KvResp> c = raftIndexCallback(callback, KvCodes.SUCCESS_OVERWRITE);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, c);
    }

    /**
     * synchronously get operation from the kv store.
     * @param groupId the raft group id
     * @param key use '.' as path separator, null or empty indicates the root node
     * @return the KvNode contains value and meta information, return null if not found
     */
    public KvNode get(int groupId, byte[] key) {
        CompletableFuture<KvNode> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        get(groupId, key, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    /**
     * asynchronously get operation from the kv store.
     * @param groupId the raft group id
     * @param key use '.' as path separator, null or empty indicates the root node
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     */
    public void get(int groupId, byte[] key, FutureCallback<KvNode> callback) {
        get(groupId, key, raftClient.createDefaultTimeout(), callback);
    }

    private void get(int groupId, byte[] key, DtTime timeout, FutureCallback<KvNode> callback) {
        checkKey(key, true);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_GET);

        raftClient.sendRequest(groupId, wf, DECODER, timeout, (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.SUCCESS || bc == KvCodes.NOT_FOUND) {
                    KvResp resp = result.getBody();
                    KvNode n = (resp == null || resp.results == null || resp.results.isEmpty()) ? null : resp.results.get(0).getNode();
                    FutureCallback.callSuccess(callback, n);
                } else {
                    FutureCallback.callFail(callback, new NetBizCodeException(bc, KvCodes.toStr(result.getBizCode())));
                }
            }
        });
    }

    /**
     * synchronously list operation from the kv store.
     * @param groupId the raft group id
     * @param key use '.' as path separator, null or empty indicates the root node
     * @return the KvNode contains value and meta information, return null if the key is not found
     */
    public List<KvResult> list(int groupId, byte[] key) {
        CompletableFuture<List<KvResult>> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        list(groupId, key, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    /**
     * asynchronously list operation from the kv store.
     * @param groupId the raft group id
     * @param key use '.' as path separator, null or empty indicates the root node
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     */
    public void list(int groupId, byte[] key, FutureCallback<List<KvResult>> callback) {
        list(groupId, key, raftClient.createDefaultTimeout(), callback);
    }

    private void list(int groupId, byte[] key, DtTime timeout, FutureCallback<List<KvResult>> callback) {
        checkKey(key, true);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_LIST);

        raftClient.sendRequest(groupId, wf, DECODER, timeout, (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.SUCCESS) {
                    KvResp resp = result.getBody();
                    FutureCallback.callSuccess(callback, (resp == null || resp.results == null)
                            ? Collections.emptyList() : resp.results);
                } else {
                    FutureCallback.callFail(callback, new NetBizCodeException(bc, KvCodes.toStr(result.getBizCode())));
                }
            }
        });
    }

    /**
     * Synchronously remove a key from the kv store.
     * This method can be used to remove a K/V node which is temporary or permanent. However, a client can only remove
     * a temporary K/V node created by itself, otherwise it will throw a KvException with code NOT_OWNER.
     * If the key is a directory, it will only be removed if it is empty.
     * If the K/V node is not exists, do nothing and return the new raft index.
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @return raft index of this write operation, it is useless in most cases.
     * @throws KvException if the K/V node is not temporary, throws KvException with code NOT_TEMP_NODE;
     *                     if the K/V node is temporary but created by another client, throws KvException with code NOT_OWNER.
     * @throws com.github.dtprj.dongting.net.NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public long remove(int groupId, byte[] key) throws KvException, NetException {
        CompletableFuture<Long> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        remove(groupId, key, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    /**
     * asynchronously remove a key from the kv store.
     * This method can be used to remove a K/V node which is temporary or permanent. However, a client can only remove
     * a temporary K/V node created by itself, otherwise the callback will complete with a KvException with code NOT_OWNER.
     * If the K/V node is not exists, do nothing and complete the callback with the new raft index.
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     */
    public void remove(int groupId, byte[] key, FutureCallback<Long> callback) {
        remove(groupId, key, raftClient.createDefaultTimeout(), callback);
    }

    private void remove(int groupId, byte[] key, DtTime timeout, FutureCallback<Long> callback) {
        checkKey(key, false);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_REMOVE);
        RpcCallback<KvResp> c = raftIndexCallback(callback, KvCodes.NOT_FOUND);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, c);
    }

    /**
     * synchronously create a directory. If the directory already exists, do nothing and return the new raft index.
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @return raft index of this write operation, it is useless in most cases.
     * @throws KvException if the exists K/V node is temporary, throws KvException with code IS_TEMP_NODE;
     * @throws com.github.dtprj.dongting.net.NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public long mkdir(int groupId, byte[] key) throws KvException, NetException {
        CompletableFuture<Long> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        mkdir(groupId, key, -1, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    /**
     * asynchronously create a directory. If the directory already exists, do nothing and complete the callback with
     * the new raft index.
     * @param groupId the raft group id throws KvException, NetException {
     * @param key not null or empty, use '.' as path separator
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     */
    public void mkdir(int groupId, byte[] key, FutureCallback<Long> callback) {
        mkdir(groupId, key, -1, raftClient.createDefaultTimeout(), callback);
    }

    /**
     * Synchronously create a temporary directory with a ttl. If the directory already exists, and it's a temporary
     * directory created by this client, update ttl and return the new raft index.
     *
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @param ttlMillis time to live in milliseconds, must be positive
     * @return raft index of this write operation, it is useless in most cases.
     * @throws KvException If the exists K/V node is not temporary, throws KvException with code NOT_TEMP_NODE;
     *                     If try to update K/V node created by another client, throws KvException with code NOT_OWNER.
     * @throws com.github.dtprj.dongting.net.NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public long makeTempDir(int groupId, byte[] key, long ttlMillis) throws KvException, NetException {
        DtUtil.checkPositive(ttlMillis, "ttlMillis");
        CompletableFuture<Long> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        mkdir(groupId, key, ttlMillis, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    /**
     * Asynchronously create a temporary directory with a ttl. If the directory already exists, and it's a temporary
     * directory created by this client, update ttl and complete the callback with the new raft index.
     *
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @param ttlMillis time to live in milliseconds, must be positive
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     */
    public void makeTempDir(int groupId, byte[] key, long ttlMillis, FutureCallback<Long> callback) {
        mkdir(groupId, key, ttlMillis, raftClient.createDefaultTimeout(), callback);
    }

    private void mkdir(int groupId, byte[] key, long ttlMillis, DtTime timeout, FutureCallback<Long> callback) {
        checkKey(key, false);
        KvReq r = new KvReq(groupId, key, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(ttlMillis > 0 ? Commands.DTKV_MAKE_TEMP_DIR : Commands.DTKV_MKDIR);
        RpcCallback<KvResp> c = raftIndexCallback(callback, KvCodes.DIR_EXISTS);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, c);
    }

    /**
     * synchronously batch put operation.
     * @param groupId the raft group id
     * @param keys list of keys, key should not null or empty, use '.' as path separator
     * @param values list of values, not null or empty
     * @return KvResp contains raft index of this operation and a list of KvResult,
     *         you may check bizCode of each KvResult
     * @see KvCodes
     */
    public KvResp batchPut(int groupId, List<byte[]> keys, List<byte[]> values) throws KvException, NetException {
        CompletableFuture<KvResp> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        batchPut(groupId, keys, values, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    /**
     * asynchronously batch put operation. The KvResp in callback contains raft index of this operation and
     * a list of KvResult, you may check bizCode of each KvResult.
     * @param groupId the raft group id
     * @param keys list of keys, key should not null or empty, use '.' as path separator
     * @param values list of values, not null or empty
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     * @see KvCodes
     */
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
        for (byte[] key : keys) {
            checkKey(key, false);
        }
        for (byte[] value : values) {
            notNullOrEmpty(value, "value");
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
                if (bc == KvCodes.SUCCESS) {
                    KvResp resp = result.getBody();
                    FutureCallback.callSuccess(callback, resp);
                } else {
                    FutureCallback.callFail(callback, new NetBizCodeException(bc, KvCodes.toStr(result.getBizCode())));
                }
            }
        };
    }

    /**
     * synchronously batch get operation.
     * @param groupId the raft group id
     * @param keys list of keys, use '.' as path separator
     * @return list of KvNode contains values and meta information, if a key not found,
     *         the corresponding KvNode will be null.
     */
    public List<KvNode> batchGet(int groupId, List<byte[]> keys) {
        CompletableFuture<List<KvNode>> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        batchGet(groupId, keys, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    /**
     * asynchronously batch get operation. The callback gives a list of KvNode contains values and meta information,
     * if a key not found, the corresponding KvNode will be null.
     * @param groupId the raft group id
     * @param keys list of keys, use '.' as path separator
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     */
    public void batchGet(int groupId, List<byte[]> keys, FutureCallback<List<KvNode>> callback) {
        batchGet(groupId, keys, raftClient.createDefaultTimeout(), callback);
    }

    private void batchGet(int groupId, List<byte[]> keys, DtTime timeout, FutureCallback<List<KvNode>> callback) {
        Objects.requireNonNull(keys);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("keys must not be empty");
        }
        for (byte[] key : keys) {
            checkKey(key, true);
        }
        KvReq r = new KvReq(groupId, keys, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_BATCH_GET);

        raftClient.sendRequest(groupId, wf, DECODER, timeout, (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else {
                int bc = result.getBizCode();
                if (bc == KvCodes.SUCCESS) {
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

    /**
     * synchronously batch remove operation.
     * @param groupId the raft group id
     * @param keys list of keys, key should not null or empty, use '.' as path separator
     * @return KvResp contains raft index of this operation and a list of KvResult,
     *         you may check bizCode of each KvResult
     * @see KvCodes
     */
    public KvResp batchRemove(int groupId, List<byte[]> keys) throws KvException, NetException {
        CompletableFuture<KvResp> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        batchRemove(groupId, keys, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    /**
     * asynchronously batch remove operation. The KvResp in callback contains raft index of this operation
     * and a list of KvResult, you may check bizCode of each KvResult.
     * @param groupId the raft group id
     * @param keys list of keys, key should not null or empty, use '.' as path separator
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     * @see KvCodes
     */
    public void batchRemove(int groupId, List<byte[]> keys, FutureCallback<KvResp> callback) {
        batchRemove(groupId, keys, raftClient.createDefaultTimeout(), callback);
    }

    private void batchRemove(int groupId, List<byte[]> keys, DtTime timeout, FutureCallback<KvResp> callback) {
        Objects.requireNonNull(keys);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("keys must not be empty");
        }
        for (byte[] key : keys) {
            checkKey(key, false);
        }
        KvReq r = new KvReq(groupId, keys, null);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_BATCH_REMOVE);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, kvRespCallback(callback));
    }

    /**
     * Synchronously compare and set operation.
     * This operation can't be used to create a directory or operate on a temporary node.
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @param expectValue the expected value, null or empty indicates the key not exist
     * @param newValue the new value, null or empty indicates delete the key
     * @return true if the operation is successful
     */
    public boolean compareAndSet(int groupId, byte[] key, byte[] expectValue, byte[] newValue) throws KvException, NetException {
        CompletableFuture<Pair<Long, Integer>> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        compareAndSet(groupId, key, expectValue, newValue, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout).getRight() == KvCodes.SUCCESS;
    }

    /**
     * Asynchronously compare and set operation.
     * This operation can't be used to create a directory or operate on a temporary node.
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @param expectValue the expected value, null or empty indicates the key not exist
     * @param newValue the new value, null or empty indicates delete the key
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     */
    public void compareAndSet(int groupId, byte[] key, byte[] expectValue, byte[] newValue,
                              FutureCallback<Pair<Long, Integer>> callback) {
        compareAndSet(groupId, key, expectValue, newValue, raftClient.createDefaultTimeout(), callback);
    }

    private void compareAndSet(int groupId, byte[] key, byte[] expectValue, byte[] newValue,
                               DtTime timeout, FutureCallback<Pair<Long, Integer>> callback) {
        checkKey(key, false);
        if ((expectValue == null || expectValue.length == 0) && (newValue == null || newValue.length == 0)) {
            throw new IllegalArgumentException("expectValue and newValue can't both be null or empty");
        }
        KvReq r = new KvReq(groupId, key, newValue, expectValue);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_CAS);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, (result, ex) -> {
            if (ex != null) {
                FutureCallback.callFail(callback, ex);
            } else {
                int bc = result.getBizCode();
                long raftIndex = result.getBody().raftIndex;
                FutureCallback.callSuccess(callback, new Pair<>(raftIndex, bc));
            }
        });
    }

    /**
     * Synchronously update the ttl of a temporary node.
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @param ttlMillis time to live in milliseconds, must be positive
     * @return raft index of this write operation, it is useless in most cases.
     * @throws KvException If the key is not a temporary node, throws KvException with code NOT_TEMP_NODE;
     *                     If try to update K/V node created by another client, throws KvException with code NOT_OWNER.
     * @throws com.github.dtprj.dongting.net.NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public long updateTtl(int groupId, byte[] key, long ttlMillis) throws KvException, NetException {
        CompletableFuture<Long> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();
        updateTtl(groupId, key, ttlMillis, timeout, FutureCallback.fromFuture(f));
        return waitFuture(f, timeout);
    }

    /**
     * Asynchronously update the ttl of a temporary node.
     * @param groupId the raft group id
     * @param key not null or empty, use '.' as path separator
     * @param ttlMillis time to live in milliseconds, must be positive
     * @param callback the callback to be called when the operation is finished. It is important to note that callbacks
     *                 for asynchronous operations may be executed on NioClient worker thread (single thread).
     *                 Therefore, you should never perform any blocking or CPU-intensive operations within
     *                 these callbacks.
     */
    public void updateTtl(int groupId, byte[] key, long ttlMillis, FutureCallback<Long> callback) {
        updateTtl(groupId, key, ttlMillis, raftClient.createDefaultTimeout(), callback);
    }

    private void updateTtl(int groupId, byte[] key, long ttlMillis, DtTime timeout, FutureCallback<Long> callback) {
        checkKey(key, false);
        DtUtil.checkPositive(ttlMillis, "ttlMillis");
        KvReq r = new KvReq(groupId, key, null, ttlMillis);
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(r);
        wf.setCommand(Commands.DTKV_UPDATE_TTL);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, raftIndexCallback(callback, KvCodes.SUCCESS));
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

    public ClientWatchManager getClientWatchManager() {
        return clientWatchManager;
    }
}

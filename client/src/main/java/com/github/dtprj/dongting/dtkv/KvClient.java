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
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.NetException;
import com.github.dtprj.dongting.net.NetTimeoutException;
import com.github.dtprj.dongting.net.NioClientConfig;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.raft.RaftClient;
import com.github.dtprj.dongting.raft.RaftClientConfig;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * DtKv client, all read operations are lease-read and still linearizable.
 *
 * @author huangli
 */
public class KvClient extends AbstractLifeCircle {
    private static final DtLog log = DtLogs.getLogger(KvClient.class);
    final RaftClient raftClient;
    private final WatchManager watchManager;
    private final LockManager lockManager = new LockManager(this);

    private static final DecoderCallbackCreator<KvResp> DECODER = ctx -> ctx.toDecoderCallback(new KvResp.Callback());

    public KvClient() {
        this(new KvClientConfig(), new RaftClientConfig(), new NioClientConfig());
    }

    public KvClient(@SuppressWarnings("unused") KvClientConfig config, RaftClientConfig raftClientConfig,
                    NioClientConfig nioConfig) {
        Objects.requireNonNull(config);
        this.raftClient = new RaftClient(raftClientConfig, nioConfig);
        this.watchManager = createClientWatchManager();
        // use bizExecutor in NioClient
        KvClientProcessor clientProcessor = new KvClientProcessor(watchManager, lockManager);
        raftClient.getNioClient().register(Commands.DTKV_WATCH_NOTIFY_PUSH, clientProcessor);
        raftClient.getNioClient().register(Commands.DTKV_LOCK_PUSH, clientProcessor);
    }

    protected WatchManager createClientWatchManager() {
        return new WatchManager(this, () -> getStatus() >= STATUS_PREPARE_STOP, 60_000);
    }

    private static boolean isSuccess(int cmd, int bizCode) {
        switch (cmd) {
            case Commands.DTKV_PUT:
            case Commands.DTKV_PUT_TEMP_NODE:
                return bizCode == KvCodes.SUCCESS || bizCode == KvCodes.SUCCESS_OVERWRITE;
            case Commands.DTKV_GET:
            case Commands.DTKV_REMOVE:
                return bizCode == KvCodes.SUCCESS || bizCode == KvCodes.NOT_FOUND;
            case Commands.DTKV_MKDIR:
            case Commands.DTKV_MAKE_TEMP_DIR:
                return bizCode == KvCodes.SUCCESS || bizCode == KvCodes.DIR_EXISTS;
            case Commands.DTKV_LIST:
            case Commands.DTKV_BATCH_PUT:
            case Commands.DTKV_BATCH_GET:
            case Commands.DTKV_BATCH_REMOVE:
            case Commands.DTKV_UPDATE_TTL:
                return bizCode == KvCodes.SUCCESS;
            case Commands.DTKV_CAS:
                return true;
            default:
                log.error("unknown cmd: {}", cmd);
                return false;
        }
    }

    protected <T> void sendAsync(int groupId, int cmd, KvReq req, FutureCallback<T> c,
                                 Function<ReadPacket<KvResp>, T> mapper) {
        DtTime timeout = raftClient.createDefaultTimeout();
        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(cmd, req);

        raftClient.sendRequest(groupId, wf, DECODER, timeout, (result, ex) ->
                asyncCallback(cmd, c, mapper, result, ex));
    }

    private <T> void asyncCallback(int cmd, FutureCallback<T> c, Function<ReadPacket<KvResp>, T> mapper,
                                   ReadPacket<KvResp> result, Throwable ex) {
        if (ex != null) {
            FutureCallback.callFail(c, ex);
        } else {
            int bc = result.bizCode;
            if (isSuccess(cmd, bc)) {
                FutureCallback.callSuccess(c, mapper.apply(result));
            } else {
                FutureCallback.callFail(c, new KvException(bc));
            }
        }
    }

    private <T> T waitFuture(CompletableFuture<T> f, DtTime timeout) {
        try {
            return f.get(timeout.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            DtUtil.restoreInterruptStatus();
            throw new NetException("interrupted", e);
        } catch (ExecutionException e) {
            throw new NetException(e);
        } catch (TimeoutException e) {
            throw new NetTimeoutException("timeout: " + timeout.getTimeout(TimeUnit.MILLISECONDS) + "ms", e);
        }
    }

    protected ReadPacket<KvResp> sendSync(int groupId, int cmd, KvReq req) {
        CompletableFuture<ReadPacket<KvResp>> f = new CompletableFuture<>();
        DtTime timeout = raftClient.createDefaultTimeout();

        EncodableBodyWritePacket wf = new EncodableBodyWritePacket(cmd, req);
        raftClient.sendRequest(groupId, wf, DECODER, timeout, RpcCallback.fromFuture(f));

        ReadPacket<KvResp> p = waitFuture(f, timeout);
        if (isSuccess(cmd, p.bizCode)) {
            return p;
        }
        // fill stack trace in caller's thread
        throw new KvException(p.bizCode);
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
        if (bs[0] == KvClientConfig.SEPARATOR || bs[bs.length - 1] == KvClientConfig.SEPARATOR) {
            return KvCodes.INVALID_KEY;
        }
        if (fullCheck) {
            int lastSep = -1;
            for (int len = bs.length, i = 0; i < len; i++) {
                if (bs[i] == ' ') {
                    return KvCodes.INVALID_KEY;
                }
                if (bs[i] == KvClientConfig.SEPARATOR) {
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
        int c = checkKey(key, KvClientConfig.MAX_KEY_SIZE, allowEmpty, true);
        if (c != KvCodes.SUCCESS) {
            throw new IllegalArgumentException("invalid key: " + KvCodes.toStr(c));
        }
    }

    /**
     * Synchronously put a key-value pair into the kv store.
     *
     * @param groupId the raft group id
     * @param key     not null or empty, use '.' as path separator
     * @param value   not null or empty
     * @throws KvException  If K/V node already exists with a tll, throws KvException with code IS_TEMP_NODE;
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public void put(int groupId, byte[] key, byte[] value) throws KvException, NetException {
        checkKey(key, false);
        notNullOrEmpty(value, "value");
        KvReq r = new KvReq(groupId, key, value);
        sendSync(groupId, Commands.DTKV_PUT, r);
    }

    /**
     * Asynchronously put a key-value pair into the kv store.
     * If K/V node already exists with a tll, the callback will complete with a KvException with code IS_TEMP_NODE.
     *
     * @param groupId  the raft group id
     * @param key      not null or empty, use '.' as path separator
     * @param value    not null or empty
     * @param callback the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    public void put(int groupId, byte[] key, byte[] value, FutureCallback<Void> callback) {
        checkKey(key, false);
        notNullOrEmpty(value, "value");
        KvReq r = new KvReq(groupId, key, value);
        sendAsync(groupId, Commands.DTKV_PUT, r, callback, v -> null);
    }

    /**
     * Synchronously put a key-value pair into the kv store, with a ttl.
     * If K/V node already exists with a tll, it will be overwritten and ttl will be updated.
     * Only the client who created the temporary K/V node can update or remove it.
     *
     * @param groupId the raft group id
     * @param key     not null or empty, use '.' as path separator
     * @param value   not null or empty
     * @throws KvException  If K/V node already exists without a tll, throws KvException with code NOT_TEMP_NODE;
     *                      If try to update K/V node created by another client, throws KvException with code NOT_OWNER.
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public void putTemp(int groupId, byte[] key, byte[] value, long ttlMillis) throws KvException, NetException {
        DtUtil.checkPositive(ttlMillis, "ttlMillis");
        checkKey(key, false);
        notNullOrEmpty(value, "value");

        KvReq r = new KvReq(groupId, key, value, ttlMillis);
        sendSync(groupId, Commands.DTKV_PUT_TEMP_NODE, r);
    }

    /**
     * Asynchronously put a key-value pair into the kv store, with a ttl.
     * Only the client who created the temporary K/V node can update or remove it.
     * If K/V node already exists with a tll, it will be overwritten and ttl will be updated.
     * If K/V node already exists without a tll, the callback will complete with a KvException with code NOT_TEMP_NODE.
     * If try to update K/V node created by another client, the callback will complete with a KvException with code NOT_OWNER.
     *
     * @param groupId   the raft group id
     * @param key       not null or empty, use '.' as path separator
     * @param value     not null or empty
     * @param ttlMillis time to live in milliseconds, must be positive
     * @param callback  the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    public void putTemp(int groupId, byte[] key, byte[] value, long ttlMillis, FutureCallback<Void> callback) {
        DtUtil.checkPositive(ttlMillis, "ttlMillis");
        checkKey(key, false);
        notNullOrEmpty(value, "value");

        KvReq r = new KvReq(groupId, key, value, ttlMillis);
        sendAsync(groupId, Commands.DTKV_PUT_TEMP_NODE, r, callback, v -> null);
    }

    /**
     * synchronously get operation from the kv store.
     *
     * @param groupId the raft group id
     * @param key     use '.' as path separator, null or empty indicates the root node
     * @return the KvNode contains value and meta information, return null if not found
     * @throws KvException  any biz exception
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public KvNode get(int groupId, byte[] key) throws KvException, NetException {
        checkKey(key, true);
        KvReq r = new KvReq(groupId, key, null);
        return mapToKvNode(sendSync(groupId, Commands.DTKV_GET, r));
    }

    /**
     * asynchronously get operation from the kv store.
     *
     * @param groupId  the raft group id
     * @param key      use '.' as path separator, null or empty indicates the root node
     * @param callback the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    public void get(int groupId, byte[] key, FutureCallback<KvNode> callback) {
        checkKey(key, true);
        KvReq req = new KvReq(groupId, key, null);
        sendAsync(groupId, Commands.DTKV_GET, req, callback, KvClient::mapToKvNode);
    }

    private static KvNode mapToKvNode(ReadPacket<KvResp> p) {
        KvResp resp = p.getBody();
        if (resp == null || resp.results == null || resp.results.isEmpty()) {
            return null;
        } else {
            return resp.results.get(0).getNode();
        }
    }

    /**
     * synchronously list operation from the kv store.
     *
     * @param groupId the raft group id
     * @param key     use '.' as path separator, null or empty indicates the root node
     * @return the KvNode list contains value and meta information, return empty list if no child
     * @throws KvException  if the key is not a directory, throws KvException with code PARENT_NOT_DIR.
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public List<KvResult> list(int groupId, byte[] key) throws KvException, NetException {
        checkKey(key, true);
        KvReq r = new KvReq(groupId, key, null);
        return mapToKvResultList(sendSync(groupId, Commands.DTKV_LIST, r));
    }

    /**
     * asynchronously list operation from the kv store.
     * if the key is not a directory, the callback will complete with a KvException with code PARENT_NOT_DIR.
     *
     * @param groupId  the raft group id
     * @param key      use '.' as path separator, null or empty indicates the root node
     * @param callback the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    public void list(int groupId, byte[] key, FutureCallback<List<KvResult>> callback) {
        checkKey(key, true);
        KvReq r = new KvReq(groupId, key, null);
        sendAsync(groupId, Commands.DTKV_LIST, r, callback, KvClient::mapToKvResultList);
    }

    private static List<KvResult> mapToKvResultList(ReadPacket<KvResp> p) {
        KvResp resp = p.getBody();
        if (resp == null || resp.results == null) {
            return Collections.emptyList();
        } else {
            return resp.results;
        }
    }

    /**
     * Synchronously remove a key from the kv store.
     * This method can be used to remove a K/V node which is temporary or permanent. However, a client can only remove
     * a temporary K/V node created by itself, otherwise it will throw a KvException with code NOT_OWNER.
     * If the key is a directory, it will only be removed if it is empty.
     * If the K/V node is not exists, do nothing.
     *
     * @param groupId the raft group id
     * @param key     not null or empty, use '.' as path separator
     * @throws KvException  if the K/V node is temporary but created by another client, throws KvException with code NOT_OWNER.
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public void remove(int groupId, byte[] key) throws KvException, NetException {
        checkKey(key, false);
        KvReq r = new KvReq(groupId, key, null);
        sendSync(groupId, Commands.DTKV_REMOVE, r);
    }

    /**
     * asynchronously remove a key from the kv store.
     * This method can be used to remove a K/V node which is temporary or permanent. However, a client can only remove
     * a temporary K/V node created by itself, otherwise the callback will complete with a KvException with code NOT_OWNER.
     * If the K/V node is not exists, do nothing.
     *
     * @param groupId  the raft group id
     * @param key      not null or empty, use '.' as path separator
     * @param callback the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    public void remove(int groupId, byte[] key, FutureCallback<Void> callback) {
        checkKey(key, false);

        KvReq r = new KvReq(groupId, key, null);
        sendAsync(groupId, Commands.DTKV_REMOVE, r, callback, v -> null);
    }

    /**
     * synchronously create a directory. If the directory already exists, do nothing.
     *
     * @param groupId the raft group id
     * @param key     not null or empty, use '.' as path separator
     * @throws KvException  if the exists K/V node is temporary, throws KvException with code IS_TEMP_NODE;
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public void mkdir(int groupId, byte[] key) throws KvException, NetException {
        checkKey(key, false);

        KvReq r = new KvReq(groupId, key, null);
        sendSync(groupId, Commands.DTKV_MKDIR, r);
    }

    /**
     * asynchronously create a directory. If the directory already exists, do nothing.
     *
     * @param groupId  the raft group id throws KvException, NetException {
     * @param key      not null or empty, use '.' as path separator
     * @param callback the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    public void mkdir(int groupId, byte[] key, FutureCallback<Void> callback) {
        checkKey(key, false);
        KvReq r = new KvReq(groupId, key, null);
        sendAsync(groupId, Commands.DTKV_MKDIR, r, callback, v -> null);
    }

    /**
     * Synchronously create a temporary directory with a ttl.
     * <p>
     * Only the client who created the temporary directory can update ttl or remove it. Temporary directory can
     * contain normal nodes (anyone can write) or temporary nodes, all sub-nodes will be removed when the temporary
     * dir expired.
     * <p>
     * If the directory already exists, and it's a temporary directory created by this client, update ttl and
     * return the new raft index.
     *
     * @param groupId   the raft group id
     * @param key       not null or empty, use '.' as path separator
     * @param ttlMillis time to live in milliseconds, must be positive
     * @throws KvException  If the exists K/V node is not temporary, throws KvException with code NOT_TEMP_NODE;
     *                      If try to update K/V node created by another client, throws KvException with code NOT_OWNER.
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public void makeTempDir(int groupId, byte[] key, long ttlMillis) throws KvException, NetException {
        DtUtil.checkPositive(ttlMillis, "ttlMillis");
        checkKey(key, false);
        KvReq r = new KvReq(groupId, key, null, ttlMillis);
        sendSync(groupId, Commands.DTKV_MAKE_TEMP_DIR, r);
    }

    /**
     * Asynchronously create a temporary directory with a ttl.
     * <p>
     * Only the client who created the temporary directory can update ttl or remove it. Temporary directory can
     * contain normal nodes (anyone can write) or temporary nodes, all sub-nodes will be removed when the temporary
     * dir expired.
     * <p>
     * If the directory already exists, and it's a temporary directory created by this client, update ttl and
     * complete the callback with the new raft index.
     *
     * @param groupId   the raft group id
     * @param key       not null or empty, use '.' as path separator
     * @param ttlMillis time to live in milliseconds, must be positive
     * @param callback  the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    public void makeTempDir(int groupId, byte[] key, long ttlMillis, FutureCallback<Void> callback) {
        DtUtil.checkPositive(ttlMillis, "ttlMillis");
        checkKey(key, false);
        KvReq r = new KvReq(groupId, key, null, ttlMillis);
        sendAsync(groupId, Commands.DTKV_MAKE_TEMP_DIR, r, callback, v -> null);
    }

    /**
     * synchronously batch put operation.
     *
     * @param groupId the raft group id
     * @param keys    list of keys, key should not null or empty, use '.' as path separator
     * @param values  list of values, not null or empty
     * @return list of KvResult, you may check bizCode of each KvResult
     * @throws KvException  any biz exception
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     * @see KvCodes
     */
    public List<KvResult> batchPut(int groupId, List<byte[]> keys, List<byte[]> values) throws KvException, NetException {
        checkBatchPut(keys, values);
        KvReq r = new KvReq(groupId, keys, values);
        return mapToKvResultList(sendSync(groupId, Commands.DTKV_BATCH_PUT, r));
    }

    /**
     * asynchronously batch put operation.
     *
     * @param groupId  the raft group id
     * @param keys     list of keys, key should not null or empty, use '.' as path separator
     * @param values   list of values, not null or empty
     * @param callback the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     * @see KvCodes
     */
    public void batchPut(int groupId, List<byte[]> keys, List<byte[]> values, FutureCallback<List<KvResult>> callback) {
        checkBatchPut(keys, values);
        KvReq r = new KvReq(groupId, keys, values);
        sendAsync(groupId, Commands.DTKV_BATCH_PUT, r, callback, KvClient::mapToKvResultList);
    }

    private void checkBatchPut(List<byte[]> keys, List<byte[]> values) {
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
    }

    /**
     * synchronously batch get operation.
     *
     * @param groupId the raft group id
     * @param keys    list of keys, use '.' as path separator
     * @return list of KvNode contains values and meta information, if a key not found,
     * the corresponding KvNode will be null.
     * @throws KvException  any biz exception
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public List<KvNode> batchGet(int groupId, List<byte[]> keys) throws KvException, NetException {
        checkBatchKeys(keys);
        KvReq r = new KvReq(groupId, keys, null);
        return mapToKvNodeList(sendSync(groupId, Commands.DTKV_BATCH_GET, r));
    }

    /**
     * asynchronously batch get operation. The callback gives a list of KvNode contains values and meta information,
     * if a key not found, the corresponding KvNode will be null.
     *
     * @param groupId  the raft group id
     * @param keys     list of keys, use '.' as path separator
     * @param callback the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    public void batchGet(int groupId, List<byte[]> keys, FutureCallback<List<KvNode>> callback) {
        checkBatchKeys(keys);
        KvReq r = new KvReq(groupId, keys, null);
        sendAsync(groupId, Commands.DTKV_BATCH_GET, r, callback, KvClient::mapToKvNodeList);
    }

    private static List<KvNode> mapToKvNodeList(ReadPacket<KvResp> p) {
        KvResp kvResp = p.getBody();
        if (kvResp == null || kvResp.results == null) {
            return Collections.emptyList();
        } else {
            return kvResp.results.stream().map(KvResult::getNode).collect(Collectors.toList());
        }
    }

    private void checkBatchKeys(List<byte[]> keys) {
        Objects.requireNonNull(keys);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("keys must not be empty");
        }
        for (byte[] key : keys) {
            checkKey(key, true);
        }
    }

    /**
     * synchronously batch remove operation.
     *
     * @param groupId the raft group id
     * @param keys    list of keys, key should not null or empty, use '.' as path separator
     * @return list of KvResult, you may check bizCode of each KvResult
     * @throws KvException  any biz exception
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     * @see KvCodes
     */
    public List<KvResult> batchRemove(int groupId, List<byte[]> keys) throws KvException, NetException {
        checkBatchKeys(keys);
        KvReq r = new KvReq(groupId, keys, null);
        return mapToKvResultList(sendSync(groupId, Commands.DTKV_BATCH_REMOVE, r));
    }

    /**
     * asynchronously batch remove operation.
     *
     * @param groupId  the raft group id
     * @param keys     list of keys, key should not null or empty, use '.' as path separator
     * @param callback the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     * @see KvCodes
     */
    public void batchRemove(int groupId, List<byte[]> keys, FutureCallback<List<KvResult>> callback) {
        checkBatchKeys(keys);
        KvReq r = new KvReq(groupId, keys, null);
        sendAsync(groupId, Commands.DTKV_BATCH_REMOVE, r, callback, KvClient::mapToKvResultList);
    }

    /**
     * Synchronously compare and set operation.
     * This operation can't be used to create a directory or operate on a temporary node.
     *
     * @param groupId     the raft group id
     * @param key         not null or empty, use '.' as path separator
     * @param expectValue the expected value, null or empty indicates the key not exist
     * @param newValue    the new value, null or empty indicates delete the key
     * @return true if the operation is successful
     */
    public boolean compareAndSet(int groupId, byte[] key, byte[] expectValue, byte[] newValue) throws NetException {
        checkKey(key, false);
        if ((expectValue == null || expectValue.length == 0) && (newValue == null || newValue.length == 0)) {
            throw new IllegalArgumentException("expectValue and newValue can't both be null or empty");
        }
        KvReq r = new KvReq(groupId, key, newValue, expectValue);
        ReadPacket<KvResp> p = sendSync(groupId, Commands.DTKV_CAS, r);
        return isCasSuccess(p);
    }

    /**
     * Asynchronously compare and set operation.
     * This operation can't be used to create a directory or operate on a temporary node.
     *
     * @param groupId     the raft group id
     * @param key         not null or empty, use '.' as path separator
     * @param expectValue the expected value, null or empty indicates the key not exist
     * @param newValue    the new value, null or empty indicates delete the key
     * @param callback    the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    public void compareAndSet(int groupId, byte[] key, byte[] expectValue, byte[] newValue,
                              FutureCallback<Boolean> callback) {
        checkKey(key, false);
        if ((expectValue == null || expectValue.length == 0) && (newValue == null || newValue.length == 0)) {
            throw new IllegalArgumentException("expectValue and newValue can't both be null or empty");
        }
        KvReq r = new KvReq(groupId, key, newValue, expectValue);
        sendAsync(groupId, Commands.DTKV_CAS, r, callback, KvClient::isCasSuccess);
    }

    private static boolean isCasSuccess(ReadPacket<KvResp> p) {
        return p.bizCode == KvCodes.SUCCESS;
    }

    /**
     * Synchronously update the ttl of a temporary node.
     *
     * @param groupId   the raft group id
     * @param key       not null or empty, use '.' as path separator
     * @param ttlMillis time to live in milliseconds, must be positive
     * @throws KvException  If the key is not a temporary node, throws KvException with code NOT_TEMP_NODE;
     *                      If try to update K/V node created by another client, throws KvException with code NOT_OWNER.
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     */
    public void updateTtl(int groupId, byte[] key, long ttlMillis) throws KvException, NetException {
        checkKey(key, false);
        DtUtil.checkPositive(ttlMillis, "ttlMillis");
        KvReq r = new KvReq(groupId, key, null, ttlMillis);
        sendSync(groupId, Commands.DTKV_UPDATE_TTL, r);
    }

    /**
     * Asynchronously update the ttl of a temporary node.
     *
     * @param groupId   the raft group id
     * @param key       not null or empty, use '.' as path separator
     * @param ttlMillis time to live in milliseconds, must be positive
     * @param callback  the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    public void updateTtl(int groupId, byte[] key, long ttlMillis, FutureCallback<Void> callback) {
        checkKey(key, false);
        DtUtil.checkPositive(ttlMillis, "ttlMillis");
        KvReq r = new KvReq(groupId, key, null, ttlMillis);
        sendAsync(groupId, Commands.DTKV_UPDATE_TTL, r, callback, v -> null);
    }

    /**
     * Create a distributed lock with the given key in the specified raft group.
     * Call close() method of the returned object will remove it from the KvClient.
     *
     * @param groupId the raft group id
     * @param key     not null or empty, use '.' as path separator
     * @return the DistributedLock instance
     * @throws IllegalStateException if an DistributedLock or AutoRenewLock instance exists with the same key
     */
    public DistributedLock createLock(int groupId, byte[] key) throws IllegalStateException {
        checkKey(key, false);
        return lockManager.createLock(groupId, key, null);
    }

    /**
     * Create a distributed lock with the given key in the specified raft group.
     * Call close() method of the returned object will remove it from the KvClient.
     *
     * @param groupId the raft group id
     * @param key     not null or empty, use '.' as path separator
     * @param expireListener the listener which will be called when the lock expires (unlock will not be called),
     *                       this listener is running in a lock, don't do blocking operations in the listener.
     * @return the DistributedLock instance
     * @throws IllegalStateException if an DistributedLock or AutoRenewLock instance exists with the same key
     */
    public DistributedLock createLock(int groupId, byte[] key, Runnable expireListener) throws IllegalStateException {
        checkKey(key, false);
        return lockManager.createLock(groupId, key, expireListener);
    }

    /**
     * Create an automatically renewing distributed lock with the given key in the specified raft group.
     * The lock will be automatically renewed before it expires, until the lock is closed.
     * Call close() method of the returned object will remove it from the KvClient.
     *
     * @param groupId     the raft group id
     * @param key         not null or empty, use '.' as path separator
     * @param leaseMillis lease time in milliseconds, must be positive. If you don't know how to set it,
     *                    60000 (60 seconds) may be a good default value.
     * @param listener    the listener to receive lock events, can not be null.
     *                    this listener is running in a lock, don't do blocking operations in the listener.
     * @return the AutoRenewalLock instance
     * @throws IllegalStateException if an DistributedLock or AutoRenewalLock instance exists with the same key
     */
    public AutoRenewalLock createAutoRenewalLock(int groupId, byte[] key, long leaseMillis,
                                               AutoRenewalLockListener listener) throws IllegalStateException {
        Objects.requireNonNull(listener);
        DtUtil.checkPositive(leaseMillis, "leaseMillis");
        checkKey(key, false);
        return lockManager.createAutoRenewLock(groupId, key, leaseMillis, listener);
    }

    @Override
    protected void doStart() {
        raftClient.start();
        lockManager.executeService = raftClient.getNioClient().getBizExecutor() == null ?
                DtUtil.SCHEDULED_SERVICE : raftClient.getNioClient().getBizExecutor();
    }

    protected void doStop(DtTime timeout, boolean force) {
        lockManager.removeAllLock();
        watchManager.removeAllWatch();
        raftClient.stop(timeout);
    }

    public RaftClient getRaftClient() {
        return raftClient;
    }

    public WatchManager getWatchManager() {
        return watchManager;
    }
}

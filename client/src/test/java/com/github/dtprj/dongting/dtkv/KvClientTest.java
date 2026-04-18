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

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.ReadPacket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
class KvClientTest {

    private static final int GROUP_ID = 1;

    // ==================== Helper Methods ====================

    private static byte[] bs(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    private static ReadPacket<KvResp> createResponse(int bizCode, KvResp body) {
        ReadPacket<KvResp> p = new ReadPacket<>();
        p.bizCode = bizCode;
        p.setBody(body);
        return p;
    }

    private static ReadPacket<KvResp> createEmptySuccessResp() {
        return createResponse(KvCodes.SUCCESS, null);
    }

    private static ReadPacket<KvResp> createKvNodeResp(byte[] data) {
        KvNode node = new KvNode(1, 1000, 2, 2000, 0, data);
        KvResult result = new KvResult(KvCodes.SUCCESS, node, null);
        KvResp body = new KvResp(10, Collections.singletonList(result));
        return createResponse(KvCodes.SUCCESS, body);
    }

    private static ReadPacket<KvResp> createKvResultListResp(KvResult... results) {
        KvResp body = new KvResp(10, Arrays.asList(results));
        return createResponse(KvCodes.SUCCESS, body);
    }

    // ==================== MockKvClient ====================

    static class MockKvClient extends KvClient {
        int capturedCmd;
        KvReq capturedReq;
        private ReadPacket<KvResp> syncResponse;
        private RuntimeException syncException;
        private ReadPacket<KvResp> asyncResponse;
        private Throwable asyncException;

        MockKvClient() {
            super();
        }

        void setSyncResponse(ReadPacket<KvResp> resp) {
            this.syncResponse = resp;
        }

        void setSyncException(RuntimeException ex) {
            this.syncException = ex;
        }

        void setAsyncResponse(ReadPacket<KvResp> resp) {
            this.asyncResponse = resp;
        }

        void setAsyncException(Throwable ex) {
            this.asyncException = ex;
        }

        @Override
        protected ReadPacket<KvResp> sendSync(int groupId, int cmd, KvReq req) {
            this.capturedCmd = cmd;
            this.capturedReq = req;
            if (syncException != null) throw syncException;
            // replicate isSuccess check from real sendSync
            if (!isSuccess(cmd, syncResponse.bizCode)) {
                throw new KvException(syncResponse.bizCode);
            }
            return syncResponse;
        }

        @Override
        protected <T> void sendAsync(int groupId, int cmd, KvReq req,
                                     FutureCallback<T> c,
                                     Function<ReadPacket<KvResp>, T> mapper) {
            this.capturedCmd = cmd;
            this.capturedReq = req;
            asyncCallback(cmd, c, mapper, asyncResponse, asyncException);
        }

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop(DtTime timeout, boolean force) {
        }
    }

    private MockKvClient createClient() {
        return new MockKvClient();
    }

    // ==================== A. checkKey Parameterized Tests ====================

    @ParameterizedTest
    @MethodSource("checkKeyProvider")
    void checkKey(byte[] key, boolean allowEmpty, boolean fullCheck, int expected) {
        assertEquals(expected, KvClient.checkKey(key, KvClientConfig.MAX_KEY_SIZE, allowEmpty, fullCheck));
    }

    static Stream<Arguments> checkKeyProvider() {
        byte[] tooLong = new byte[KvClientConfig.MAX_KEY_SIZE + 1];
        Arrays.fill(tooLong, (byte) 'a');

        return Stream.of(
                // null/empty with allowEmpty
                Arguments.of(null, true, true, KvCodes.SUCCESS),
                Arguments.of(null, false, true, KvCodes.INVALID_KEY),
                Arguments.of(new byte[0], true, true, KvCodes.SUCCESS),
                Arguments.of(new byte[0], false, true, KvCodes.INVALID_KEY),
                // too long
                Arguments.of(tooLong, false, true, KvCodes.KEY_TOO_LONG),
                // starts/ends with separator
                Arguments.of(bs(".abc"), false, true, KvCodes.INVALID_KEY),
                Arguments.of(bs("abc."), false, true, KvCodes.INVALID_KEY),
                // contains space
                Arguments.of(bs("a b"), false, true, KvCodes.INVALID_KEY),
                // consecutive separators
                Arguments.of(bs("a..b"), false, true, KvCodes.INVALID_KEY),
                // valid keys
                Arguments.of(bs("abc"), false, true, KvCodes.SUCCESS),
                Arguments.of(bs("a.b.c"), false, true, KvCodes.SUCCESS),
                // fullCheck=false skips space/separator checks
                Arguments.of(bs("a b"), false, false, KvCodes.SUCCESS),
                Arguments.of(bs("a..b"), false, false, KvCodes.SUCCESS),
                // starts/ends with separator still fails even with fullCheck=false
                Arguments.of(bs(".abc"), false, false, KvCodes.INVALID_KEY),
                Arguments.of(bs("abc."), false, false, KvCodes.INVALID_KEY)
        );
    }

    // ==================== B. Parameter Validation Tests ====================

    @Test
    void put_invalidArgs() {
        MockKvClient c = createClient();
        // null key → checkKey throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> c.put(GROUP_ID, null, bs("v")));
        // empty key
        assertThrows(IllegalArgumentException.class, () -> c.put(GROUP_ID, new byte[0], bs("v")));
        // null value
        assertThrows(NullPointerException.class, () -> c.put(GROUP_ID, bs("k"), null));
        // empty value
        assertThrows(IllegalArgumentException.class, () -> c.put(GROUP_ID, bs("k"), new byte[0]));
    }

    @Test
    void putTemp_invalidTtl() {
        MockKvClient c = createClient();
        assertThrows(IllegalArgumentException.class,
                () -> c.putTemp(GROUP_ID, bs("k"), bs("v"), 0));
        assertThrows(IllegalArgumentException.class,
                () -> c.putTemp(GROUP_ID, bs("k"), bs("v"), -1));
    }

    @Test
    void remove_invalidArgs() {
        MockKvClient c = createClient();
        // null key → checkKey throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> c.remove(GROUP_ID, null));
        assertThrows(IllegalArgumentException.class, () -> c.remove(GROUP_ID, new byte[0]));
    }

    @Test
    void get_nullKey_allowed() {
        MockKvClient c = createClient();
        c.setSyncResponse(createEmptySuccessResp());
        // null key is allowed for get (allowEmpty=true)
        assertDoesNotThrow(() -> c.get(GROUP_ID, null));
    }

    @Test
    void batchPut_invalidArgs() {
        MockKvClient c = createClient();
        // empty lists
        assertThrows(IllegalArgumentException.class,
                () -> c.batchPut(GROUP_ID, Collections.emptyList(), Collections.emptyList()));
        // mismatched sizes
        assertThrows(IllegalArgumentException.class,
                () -> c.batchPut(GROUP_ID, Collections.singletonList(bs("k")),
                        Arrays.asList(bs("v1"), bs("v2"))));
        // null key in list → checkKey throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class,
                () -> c.batchPut(GROUP_ID, Collections.singletonList(null),
                        Collections.singletonList(bs("v"))));
    }

    @Test
    void batchGet_invalidArgs() {
        MockKvClient c = createClient();
        assertThrows(IllegalArgumentException.class,
                () -> c.batchGet(GROUP_ID, Collections.emptyList()));
    }

    @Test
    void cas_invalidArgs() {
        MockKvClient c = createClient();
        // both null
        assertThrows(IllegalArgumentException.class,
                () -> c.compareAndSet(GROUP_ID, bs("k"), null, null));
        // both empty
        assertThrows(IllegalArgumentException.class,
                () -> c.compareAndSet(GROUP_ID, bs("k"), new byte[0], new byte[0]));
        // null key → checkKey throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class,
                () -> c.compareAndSet(GROUP_ID, null, bs("old"), bs("new")));
    }

    @Test
    void updateTtl_invalidTtl() {
        MockKvClient c = createClient();
        assertThrows(IllegalArgumentException.class,
                () -> c.updateTtl(GROUP_ID, bs("k"), 0));
        assertThrows(IllegalArgumentException.class,
                () -> c.updateTtl(GROUP_ID, bs("k"), -1));
    }

    @Test
    void createLock_invalidKey() {
        MockKvClient c = createClient();
        // null key → checkKey throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> c.createLock(GROUP_ID, null));
    }

    @Test
    void createAutoRenewalLock_invalidArgs() {
        MockKvClient c = createClient();
        // null listener
        assertThrows(NullPointerException.class,
                () -> c.createAutoRenewalLock(GROUP_ID, bs("k"), 60000, null));
        // zero leaseMillis
        assertThrows(IllegalArgumentException.class,
                () -> c.createAutoRenewalLock(GROUP_ID, bs("k"), 0,
                        new AutoRenewalLockListener() {
                            @Override public void onAcquired(AutoRenewalLock lock) {}
                            @Override public void onLost(AutoRenewalLock lock) {}
                        }));
    }

    @Test
    void mkdir_invalidArgs() {
        MockKvClient c = createClient();
        // null key → checkKey throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> c.mkdir(GROUP_ID, null));
        assertThrows(IllegalArgumentException.class, () -> c.mkdir(GROUP_ID, new byte[0]));
    }

    @Test
    void makeTempDir_invalidArgs() {
        MockKvClient c = createClient();
        assertThrows(IllegalArgumentException.class,
                () -> c.makeTempDir(GROUP_ID, bs("k"), 0));
        // null key → checkKey throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class,
                () -> c.makeTempDir(GROUP_ID, null, 1000));
    }

    // ==================== C. Sync Response Mapping + Request Construction ====================

    @Test
    void put_success() {
        MockKvClient c = createClient();
        c.setSyncResponse(createEmptySuccessResp());

        c.put(GROUP_ID, bs("mykey"), bs("myval"));

        assertEquals(Commands.DTKV_PUT, c.capturedCmd);
        assertEquals(GROUP_ID, c.capturedReq.groupId);
        assertArrayEquals(bs("mykey"), c.capturedReq.key);
        assertArrayEquals(bs("myval"), c.capturedReq.value);
    }

    @Test
    void put_overwrite_success() {
        MockKvClient c = createClient();
        c.setSyncResponse(createResponse(KvCodes.SUCCESS_OVERWRITE, null));

        // SUCCESS_OVERWRITE is also success for DTKV_PUT
        assertDoesNotThrow(() -> c.put(GROUP_ID, bs("k"), bs("v")));
    }

    @Test
    void put_isTempNode_fail() {
        MockKvClient c = createClient();
        c.setSyncResponse(createResponse(KvCodes.IS_TEMP_NODE, null));

        KvException ex = assertThrows(KvException.class, () -> c.put(GROUP_ID, bs("k"), bs("v")));
        assertEquals(KvCodes.IS_TEMP_NODE, ex.getCode());
    }

    @Test
    void putTemp_success() {
        MockKvClient c = createClient();
        c.setSyncResponse(createEmptySuccessResp());

        c.putTemp(GROUP_ID, bs("k"), bs("v"), 5000);

        assertEquals(Commands.DTKV_PUT_TEMP_NODE, c.capturedCmd);
        assertEquals(5000, c.capturedReq.ttlMillis);
        assertArrayEquals(bs("k"), c.capturedReq.key);
        assertArrayEquals(bs("v"), c.capturedReq.value);
    }

    @Test
    void get_success() {
        MockKvClient c = createClient();
        c.setSyncResponse(createKvNodeResp(bs("data")));

        KvNode node = c.get(GROUP_ID, bs("k"));

        assertEquals(Commands.DTKV_GET, c.capturedCmd);
        assertArrayEquals(bs("k"), c.capturedReq.key);
        assertNotNull(node);
        assertArrayEquals(bs("data"), node.data);
    }

    @Test
    void get_notFound() {
        MockKvClient c = createClient();
        c.setSyncResponse(createResponse(KvCodes.NOT_FOUND, null));

        KvNode node = c.get(GROUP_ID, bs("k"));
        assertNull(node);
    }

    @Test
    void get_nullBody() {
        MockKvClient c = createClient();
        c.setSyncResponse(createResponse(KvCodes.SUCCESS, null));

        KvNode node = c.get(GROUP_ID, bs("k"));
        assertNull(node);
    }

    @Test
    void list_success() {
        MockKvClient c = createClient();
        KvResult r1 = new KvResult(KvCodes.SUCCESS, new KvNode(1, 1, 1, 1, 0, bs("d1")), null);
        KvResult r2 = new KvResult(KvCodes.SUCCESS, new KvNode(2, 2, 2, 2, 0, bs("d2")), null);
        c.setSyncResponse(createKvResultListResp(r1, r2));

        List<KvResult> results = c.list(GROUP_ID, bs("k"));

        assertEquals(Commands.DTKV_LIST, c.capturedCmd);
        assertEquals(2, results.size());
        assertArrayEquals(bs("d1"), results.get(0).getNode().data);
        assertArrayEquals(bs("d2"), results.get(1).getNode().data);
    }

    @Test
    void list_emptyBody() {
        MockKvClient c = createClient();
        c.setSyncResponse(createResponse(KvCodes.SUCCESS, null));

        List<KvResult> results = c.list(GROUP_ID, bs("k"));
        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    void remove_success() {
        MockKvClient c = createClient();
        c.setSyncResponse(createEmptySuccessResp());

        c.remove(GROUP_ID, bs("k"));

        assertEquals(Commands.DTKV_REMOVE, c.capturedCmd);
        assertArrayEquals(bs("k"), c.capturedReq.key);
    }

    @Test
    void remove_notFound() {
        MockKvClient c = createClient();
        // NOT_FOUND is also success for DTKV_REMOVE
        c.setSyncResponse(createResponse(KvCodes.NOT_FOUND, null));

        assertDoesNotThrow(() -> c.remove(GROUP_ID, bs("k")));
    }

    @Test
    void mkdir_success() {
        MockKvClient c = createClient();
        c.setSyncResponse(createEmptySuccessResp());

        c.mkdir(GROUP_ID, bs("dir"));

        assertEquals(Commands.DTKV_MKDIR, c.capturedCmd);
        assertArrayEquals(bs("dir"), c.capturedReq.key);
    }

    @Test
    void mkdir_dirExists() {
        MockKvClient c = createClient();
        c.setSyncResponse(createResponse(KvCodes.DIR_EXISTS, null));

        // DIR_EXISTS is also success for DTKV_MKDIR
        assertDoesNotThrow(() -> c.mkdir(GROUP_ID, bs("dir")));
    }

    @Test
    void makeTempDir_success() {
        MockKvClient c = createClient();
        c.setSyncResponse(createEmptySuccessResp());

        c.makeTempDir(GROUP_ID, bs("dir"), 3000);

        assertEquals(Commands.DTKV_MAKE_TEMP_DIR, c.capturedCmd);
        assertEquals(3000, c.capturedReq.ttlMillis);
        assertArrayEquals(bs("dir"), c.capturedReq.key);
    }

    @Test
    void makeTempDir_dirExists() {
        MockKvClient c = createClient();
        c.setSyncResponse(createResponse(KvCodes.DIR_EXISTS, null));

        // DIR_EXISTS is also success for DTKV_MAKE_TEMP_DIR
        assertDoesNotThrow(() -> c.makeTempDir(GROUP_ID, bs("dir"), 3000));
    }

    @Test
    void batchPut_success() {
        MockKvClient c = createClient();
        List<byte[]> keys = Arrays.asList(bs("k1"), bs("k2"));
        List<byte[]> values = Arrays.asList(bs("v1"), bs("v2"));
        c.setSyncResponse(createKvResultListResp(KvResult.SUCCESS, KvResult.SUCCESS));

        List<KvResult> results = c.batchPut(GROUP_ID, keys, values);

        assertEquals(Commands.DTKV_BATCH_PUT, c.capturedCmd);
        assertEquals(2, results.size());
    }

    @Test
    void batchGet_success() {
        MockKvClient c = createClient();
        KvNode n1 = new KvNode(1, 1, 1, 1, 0, bs("d1"));
        KvNode n2 = new KvNode(2, 2, 2, 2, 0, bs("d2"));
        KvResult r1 = new KvResult(KvCodes.SUCCESS, n1, null);
        KvResult r2 = new KvResult(KvCodes.SUCCESS, n2, null);
        c.setSyncResponse(createKvResultListResp(r1, r2));

        List<byte[]> keys = Arrays.asList(bs("k1"), bs("k2"));
        List<KvNode> nodes = c.batchGet(GROUP_ID, keys);

        assertEquals(Commands.DTKV_BATCH_GET, c.capturedCmd);
        assertEquals(2, nodes.size());
        assertArrayEquals(bs("d1"), nodes.get(0).data);
        assertArrayEquals(bs("d2"), nodes.get(1).data);
    }

    @Test
    void batchGet_nullBody() {
        MockKvClient c = createClient();
        c.setSyncResponse(createResponse(KvCodes.SUCCESS, null));

        List<KvNode> nodes = c.batchGet(GROUP_ID, Collections.singletonList(bs("k")));
        assertNotNull(nodes);
        assertTrue(nodes.isEmpty());
    }

    @Test
    void batchRemove_success() {
        MockKvClient c = createClient();
        c.setSyncResponse(createKvResultListResp(KvResult.SUCCESS));

        List<KvResult> results = c.batchRemove(GROUP_ID, Collections.singletonList(bs("k")));

        assertEquals(Commands.DTKV_BATCH_REMOVE, c.capturedCmd);
        assertEquals(1, results.size());
    }

    @Test
    void cas_success_true() {
        MockKvClient c = createClient();
        c.setSyncResponse(createEmptySuccessResp());

        boolean result = c.compareAndSet(GROUP_ID, bs("k"), bs("old"), bs("new"));

        assertEquals(Commands.DTKV_CAS, c.capturedCmd);
        assertTrue(result);
        assertArrayEquals(bs("new"), c.capturedReq.value);
        assertArrayEquals(bs("old"), c.capturedReq.expectValue);
    }

    @Test
    void cas_mismatch_false() {
        MockKvClient c = createClient();
        c.setSyncResponse(createResponse(KvCodes.CAS_MISMATCH, null));

        // CAS_MISMATCH: isSuccess returns true, but mapper (isCasSuccess) returns false
        boolean result = c.compareAndSet(GROUP_ID, bs("k"), bs("old"), bs("new"));
        assertFalse(result);
    }

    @Test
    void updateTtl_success() {
        MockKvClient c = createClient();
        c.setSyncResponse(createEmptySuccessResp());

        c.updateTtl(GROUP_ID, bs("k"), 5000);

        assertEquals(Commands.DTKV_UPDATE_TTL, c.capturedCmd);
        assertEquals(5000, c.capturedReq.ttlMillis);
        assertArrayEquals(bs("k"), c.capturedReq.key);
    }

    // ==================== D. Async Callback Tests ====================

    @Test
    void putAsync_success() {
        MockKvClient c = createClient();
        c.setAsyncResponse(createEmptySuccessResp());

        CompletableFuture<Void> future = new CompletableFuture<>();
        c.put(GROUP_ID, bs("k"), bs("v"), FutureCallback.fromFuture(future));

        assertEquals(Commands.DTKV_PUT, c.capturedCmd);
        assertNull(future.join());
    }

    @Test
    void putAsync_errorBizCode() {
        MockKvClient c = createClient();
        c.setAsyncResponse(createResponse(KvCodes.IS_TEMP_NODE, null));

        CompletableFuture<Void> future = new CompletableFuture<>();
        c.put(GROUP_ID, bs("k"), bs("v"), FutureCallback.fromFuture(future));

        ExecutionExceptionHolder holder = getAsyncException(future);
        assertInstanceOf(KvException.class, holder.cause);
        assertEquals(KvCodes.IS_TEMP_NODE, ((KvException) holder.cause).getCode());
    }

    @Test
    void getAsync_success() {
        MockKvClient c = createClient();
        c.setAsyncResponse(createKvNodeResp(bs("data")));

        CompletableFuture<KvNode> future = new CompletableFuture<>();
        c.get(GROUP_ID, bs("k"), FutureCallback.fromFuture(future));

        KvNode node = future.join();
        assertNotNull(node);
        assertArrayEquals(bs("data"), node.data);
    }

    @Test
    void listAsync_success() {
        MockKvClient c = createClient();
        KvResult r = new KvResult(KvCodes.SUCCESS, new KvNode(1, 1, 1, 1, 0, bs("d")), null);
        c.setAsyncResponse(createKvResultListResp(r));

        CompletableFuture<List<KvResult>> future = new CompletableFuture<>();
        c.list(GROUP_ID, bs("k"), FutureCallback.fromFuture(future));

        List<KvResult> results = future.join();
        assertEquals(1, results.size());
        assertArrayEquals(bs("d"), results.get(0).getNode().data);
    }

    @Test
    void batchGetAsync_success() {
        MockKvClient c = createClient();
        KvNode n = new KvNode(1, 1, 1, 1, 0, bs("d"));
        KvResult r = new KvResult(KvCodes.SUCCESS, n, null);
        c.setAsyncResponse(createKvResultListResp(r));

        CompletableFuture<List<KvNode>> future = new CompletableFuture<>();
        c.batchGet(GROUP_ID, Collections.singletonList(bs("k")), FutureCallback.fromFuture(future));

        List<KvNode> nodes = future.join();
        assertEquals(1, nodes.size());
        assertArrayEquals(bs("d"), nodes.get(0).data);
    }

    @Test
    void casAsync_mismatch() {
        MockKvClient c = createClient();
        c.setAsyncResponse(createResponse(KvCodes.CAS_MISMATCH, null));

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        c.compareAndSet(GROUP_ID, bs("k"), bs("old"), bs("new"),
                FutureCallback.fromFuture(future));

        assertFalse(future.join());
    }

    @Test
    void async_exception() {
        MockKvClient c = createClient();
        c.setAsyncException(new RuntimeException("network error"));

        CompletableFuture<Void> future = new CompletableFuture<>();
        c.put(GROUP_ID, bs("k"), bs("v"), FutureCallback.fromFuture(future));

        ExecutionExceptionHolder holder = getAsyncException(future);
        assertEquals("network error", holder.cause.getMessage());
    }

    // ==================== E. sendSync Exception ====================

    @Test
    void sendSync_exception() {
        MockKvClient c = createClient();
        c.setSyncException(new RuntimeException("connection refused"));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> c.put(GROUP_ID, bs("k"), bs("v")));
        assertEquals("connection refused", ex.getMessage());
    }

    // ==================== F. Lifecycle ====================

    @Test
    void startAndStop() {
        MockKvClient c = createClient();
        assertEquals(AbstractLifeCircle.STATUS_NOT_START, c.getStatus());

        c.start();
        assertEquals(AbstractLifeCircle.STATUS_RUNNING, c.getStatus());

        c.stop(new DtTime(5, TimeUnit.SECONDS));
        assertEquals(AbstractLifeCircle.STATUS_STOPPED, c.getStatus());
    }

    // ==================== Helper for async exception assertions ====================

    private static class ExecutionExceptionHolder {
        final Throwable cause;
        ExecutionExceptionHolder(Throwable cause) {
            this.cause = cause;
        }
    }

    private static ExecutionExceptionHolder getAsyncException(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("Expected exception but future completed successfully");
        } catch (java.util.concurrent.CompletionException e) {
            return new ExecutionExceptionHolder(e.getCause());
        }
    }
}

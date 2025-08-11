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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.SnapshotInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public class DtKVTest extends BaseFiberTest {
    private long ver;
    private boolean useSeparateExecutor;

    private DtKV kv;
    private Timestamp ts;

    @BeforeEach
    void setUp() {
        ver = 1;
    }

    private DtKV createAndStart() throws Exception {
        ts = fiberGroup.dispatcher.ts;
        RaftGroupConfigEx groupConfig = new RaftGroupConfigEx(0, "1", "");
        groupConfig.raftStatus = new RaftStatusImpl(1, ts);
        groupConfig.fiberGroup = fiberGroup;
        groupConfig.ts = ts;
        KvConfig kvConfig = new KvConfig();
        kvConfig.useSeparateExecutor = this.useSeparateExecutor;
        kvConfig.initMapCapacity = 16;
        DtKV kv = new DtKV(groupConfig, kvConfig);
        doInFiber(kv::start);
        return kv;
    }

    private void stop(DtKV kv) {
        kv.stop(new DtTime(1, TimeUnit.SECONDS));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private FiberFuture<KvResult> put(long index, String key, String value) {
        KvReq req = new KvReq(1, key.getBytes(), value.getBytes());
        RaftInput i = new RaftInput(DtKV.BIZ_TYPE_PUT, null, req,
                new DtTime(1, TimeUnit.SECONDS), false);
        return (FiberFuture) kv.exec(index, ts.wallClockMillis,ts.nanoTime, i);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private FiberFuture<Pair<Integer, List<KvResult>>> batchPut(long index, List<String> keys, List<String> values) {
        List<byte[]> keyList = keys.stream().map(String::getBytes).collect(Collectors.toList());
        List<byte[]> valueList = values.stream().map(String::getBytes).collect(Collectors.toList());
        KvReq batchPutReq = new KvReq(1, keyList, valueList);
        RaftInput batchPutInput = new RaftInput(DtKV.BIZ_TYPE_BATCH_PUT, null, batchPutReq,
                new DtTime(1, TimeUnit.SECONDS), false);
        return (FiberFuture) kv.exec(index, ts.wallClockMillis,ts.nanoTime, batchPutInput);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private FiberFuture<KvResult> remove(long index, String key) {
        KvReq req = new KvReq(1, key.getBytes(), null);
        RaftInput i = new RaftInput(DtKV.BIZ_TYPE_REMOVE, null, req,
                new DtTime(1, TimeUnit.SECONDS), false);
        return (FiberFuture) kv.exec(index, ts.wallClockMillis,ts.nanoTime, i);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private FiberFuture<Pair<Integer, List<KvResult>>> batchRemove(long index, List<String> keys) {
        List<byte[]> keyList = keys.stream().map(String::getBytes).collect(Collectors.toList());
        KvReq batchRemoveReq = new KvReq(1, keyList, null);
        RaftInput batchRemoveInput = new RaftInput(DtKV.BIZ_TYPE_BATCH_REMOVE, null, batchRemoveReq,
                new DtTime(1, TimeUnit.SECONDS), false);
        return (FiberFuture) kv.exec(index, ts.wallClockMillis,ts.nanoTime, batchRemoveInput);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private FiberFuture<KvResult> mkdir(long index, String key) {
        KvReq req = new KvReq(1, key.getBytes(), null);
        RaftInput i = new RaftInput(DtKV.BIZ_TYPE_MKDIR, null,
                req, new DtTime(1, TimeUnit.SECONDS), false);
        return (FiberFuture) kv.exec(index, ts.wallClockMillis,ts.nanoTime, i);
    }

    private KvResult get(String key) {
        return kv.get(new ByteArray(key.getBytes()));
    }

    private KvResult get(DtKV dtkv, String key) {
        return dtkv.get(new ByteArray(key.getBytes()));
    }

    private String getStr(DtKV dtkv, String key) {
        return new String(dtkv.get(new ByteArray(key.getBytes())).getNode().data);
    }

    private Pair<Integer, List<KvResult>> list(String key) {
        return kv.list(new ByteArray(key.getBytes()));
    }

    @Test
    void simpleTest() throws Exception {
        useSeparateExecutor = false;
        kv = createAndStart();
        simpleTest0();
        stop(kv);

        useSeparateExecutor = true;
        kv = createAndStart();
        simpleTest0();
        stop(kv);
    }

    private void simpleTest0() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return mkdir(ver++, "parent").await(this::afterMkdir);
            }

            private FrameCallResult afterMkdir(KvResult kvResult) {
                assertEquals(KvCodes.CODE_SUCCESS, kvResult.getBizCode());
                return put(ver++, "parent.child1", "v1").await(this::afterPut);
            }

            private FrameCallResult afterPut(KvResult kvResult) {
                assertEquals(KvCodes.CODE_SUCCESS, kvResult.getBizCode());
                assertEquals(KvCodes.CODE_SUCCESS, get("parent.child1").getBizCode());
                assertEquals("v1", new String(get("parent.child1").getNode().data));
                return remove(ver++, "parent.child1").await(this::afterRemove);
            }

            private FrameCallResult afterRemove(KvResult kvResult) {
                assertEquals(KvCodes.CODE_SUCCESS, kvResult.getBizCode());
                assertEquals(KvCodes.CODE_SUCCESS, list("").getLeft());
                assertEquals(1, list("").getRight().size());

                // batch put
                List<String> keys = List.of("batch1", "batch2");
                List<String> values = List.of("value1", "value2");
                return batchPut(ver++, keys, values).await(this::afterBatchPut);
            }

            private FrameCallResult afterBatchPut(Pair<Integer, List<KvResult>> p) {
                assertEquals(KvCodes.CODE_SUCCESS, p.getLeft());

                // batch get
                ArrayList<byte[]> getKeys = new ArrayList<>();
                getKeys.add("batch1".getBytes());
                getKeys.add("batch2".getBytes());
                Pair<Integer, List<KvResult>> batchGetResult = kv.batchGet(getKeys);
                assertEquals(KvCodes.CODE_SUCCESS, batchGetResult.getLeft());
                assertEquals(2, batchGetResult.getRight().size());
                assertEquals("value1", new String(batchGetResult.getRight().get(0).getNode().data));
                assertEquals("value2", new String(batchGetResult.getRight().get(1).getNode().data));

                // batch remove
                return batchRemove(ver++, List.of("batch1", "batch2")).await(this::afterBatchRemove);
            }

            private FrameCallResult afterBatchRemove(Pair<Integer, List<KvResult>> p) {
                assertEquals(KvCodes.CODE_SUCCESS, p.getLeft());

                // verify batch remove
                ArrayList<byte[]> getKeys = new ArrayList<>();
                getKeys.add("batch1".getBytes());
                getKeys.add("batch2".getBytes());
                Pair<Integer, List<KvResult>> batchGetResult = kv.batchGet(getKeys);
                assertEquals(KvCodes.CODE_SUCCESS, batchGetResult.getLeft());
                assertEquals(2, batchGetResult.getRight().size());
                assertEquals(KvCodes.CODE_NOT_FOUND, batchGetResult.getRight().get(0).getBizCode());
                assertEquals(KvCodes.CODE_NOT_FOUND, batchGetResult.getRight().get(1).getBizCode());

                // compareAndSet
                put(ver++, "cas_key", "old_value");
                KvReq casReq = new KvReq(1, "cas_key".getBytes(), "new_value".getBytes(), "old_value".getBytes());
                RaftInput casInput = new RaftInput(DtKV.BIZ_TYPE_CAS, null, casReq,
                        new DtTime(1, TimeUnit.SECONDS), false);
                return kv.exec(ver++, ts.wallClockMillis,ts.nanoTime, casInput).await(this::afterCas);
            }

            private FrameCallResult afterCas(Object result) {
                KvResult casResult = (KvResult) result;
                assertEquals(KvCodes.CODE_SUCCESS, casResult.getBizCode());
                assertEquals("new_value", new String(get("cas_key").getNode().data));
                return Fiber.frameReturn();
            }
        });
    }

    private FrameCallResult takeSnapshot(FrameCall<Snapshot> resumePoint) {
        long lastIndex = ver - 1;
        int lastTerm = 1;
        SnapshotInfo si = new SnapshotInfo(lastIndex, lastTerm, null, null, null, null, 0);
        FiberFuture<Snapshot> f = kv.takeSnapshot(si);
        return f.await(resumePoint);
    }

    private DtKV copyTo(KvSnapshot s) throws Exception {
        DtKV kv2 = createAndStart();
        doInFiber(new CopyFrame(s, kv2));
        return kv2;
    }

    private static class CopyFrame extends FiberFrame<Void> {

        private final KvSnapshot s;
        private final DtKV dest;
        private final ByteBuffer buf = ByteBuffer.allocate(64);
        private long offset = 0;
        private final long lastIndex;
        private final int lastTerm;

        CopyFrame(KvSnapshot s, DtKV dest) {
            this.s = s;
            this.dest = dest;
            this.lastIndex = s.getSnapshotInfo().getLastIncludedIndex();
            this.lastTerm = s.getSnapshotInfo().getLastIncludedTerm();
        }

        @Override
        public FrameCallResult execute(Void input) {
            return dest.installSnapshot(lastIndex, lastTerm, offset, false, null).await(this::loop);
        }

        private FrameCallResult loop(Void unused) {
            buf.clear();
            return s.readNext(buf).await(this::afterRead);
        }

        private FrameCallResult afterRead(Integer bytes) {
            buf.flip();
            assertEquals(bytes, buf.remaining());
            return dest.installSnapshot(lastIndex, lastTerm, offset, false, buf).await(v -> afterInstall(bytes));
        }

        private FrameCallResult afterInstall(int bytes) {
            offset += bytes;
            if (bytes != 0) {
                return Fiber.resume(null, this::loop);
            } else {
                return lastInstall();
            }
        }

        private FrameCallResult lastInstall() {
            return dest.installSnapshot(lastIndex, lastTerm, offset, true, null).await(this::justReturn);
        }
    }

    private long[] backupIndexAndTime(String key) {
        long[] result = new long[4];
        KvResult r = get(key);
        result[0] = r.getNode().createIndex;
        result[1] = r.getNode().createTime;
        result[2] = r.getNode().updateIndex;
        result[3] = r.getNode().updateTime;
        return result;
    }

    private void checkIndexAndTime(DtKV newKv, String key, long[] indexAndTime) {
        KvResult r = get(newKv, key);
        assertEquals(indexAndTime[0], r.getNode().createIndex);
        assertEquals(indexAndTime[1], r.getNode().createTime);
        assertEquals(indexAndTime[2], r.getNode().updateIndex);
        assertEquals(indexAndTime[3], r.getNode().updateTime);
    }


    long[] root_1, d1_1, d1k2_1, d1dd1_1;
    long[] root_2, d1_2, d1k2_2, k1_2;
    long[] root_3, d1_3, k1_3, d1dd2_3;
    KvSnapshot s1, s2, s3;

    @Test
    void testSnapshot() throws Exception {
        kv = createAndStart();
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return mkdir(ver++, "d1").await(this::afterMkdir1);
            }

            private FrameCallResult afterMkdir1(KvResult kvResult) {
                return mkdir(ver++, "d1.dd1").await(this::afterMkdir2);
            }

            private FrameCallResult afterMkdir2(KvResult kvResult) {
                return mkdir(ver++, "d1.dd2").await(this::afterMkdir3);
            }

            private FrameCallResult afterMkdir3(KvResult kvResult) {
                return mkdir(ver++, "d1.dd1.ddd1").await(this::afterMkdir4);
            }

            private FrameCallResult afterMkdir4(KvResult kvResult) {
                List<String> keys = new ArrayList<>();
                List<String> values = new ArrayList<>();
                keys.add("k1");
                values.add("k1_v");
                keys.add("k2");
                values.add("k2_v");
                keys.add("d1.k1");
                values.add("d1.k1_v");
                keys.add("d1.k2");
                values.add("d1.k2_v");
                keys.add("d1.dd1.k1");
                values.add("d1.dd1.k1_v");
                keys.add("d1.dd1.k2");
                values.add("d1.dd1.k2_v");
                keys.add("d1.dd1.ddd1.k1");
                values.add("d1.dd1.ddd1.k1_v");
                keys.add("d1.dd1.ddd1.k2");
                values.add("d1.dd1.ddd1.k2_v");
                keys.add("d1.dd2.k1");
                values.add("d1.dd2.k1_v");
                keys.add("d1.dd2.k2");
                values.add("d1.dd2.k2_v");
                for (int i = 0; i < 50; i++) {
                    keys.add("key" + i);
                    values.add("value" + i);
                }
                return batchPut(ver++, keys, values).await(this::afterBatchPut);
            }

            private FrameCallResult afterBatchPut(Pair<Integer, List<KvResult>> pair) {
                root_1 = backupIndexAndTime("");
                d1_1 = backupIndexAndTime("d1");
                d1k2_1 = backupIndexAndTime("d1.k2");
                d1dd1_1 = backupIndexAndTime("d1.dd1");
                return takeSnapshot(this::afterTakeSnapshotS1);
            }

            private FrameCallResult afterTakeSnapshotS1(Snapshot s) {
                s1 = (KvSnapshot) s;
                return put(ver++, "d1.k2", "d1.k2_v2").await(this::afterPut);
            }

            private FrameCallResult afterPut(KvResult kvResult) {
                root_2 = backupIndexAndTime("");
                d1_2 = backupIndexAndTime("d1");
                d1k2_2 = backupIndexAndTime("d1.k2");
                k1_2 = backupIndexAndTime("k1");
                return takeSnapshot(this::afterTakeSnapshotS2);
            }

            private FrameCallResult afterTakeSnapshotS2(Snapshot s) {
                s2 = (KvSnapshot) s;
                return remove(ver++, "k1").await(this::afterRemove1);
            }

            private FrameCallResult afterRemove1(KvResult kvResult) {
                return mkdir(ver++, "k1").await(this::afterMkdir5);
            }

            private FrameCallResult afterMkdir5(KvResult kvResult) {
                return put(ver++, "k1.k1", "k1.k1_v").await(this::afterPut2);
            }

            private FrameCallResult afterPut2(KvResult kvResult) {
                return remove(ver++, "d1.dd2.k1").await(this::afterRemove2);
            }

            private FrameCallResult afterRemove2(KvResult kvResult) {
                return remove(ver++, "d1.dd2.k2").await(this::afterRemove3);
            }

            private FrameCallResult afterRemove3(KvResult kvResult) {
                return remove(ver++, "d1.dd2").await(this::afterRemove4);
            }

            private FrameCallResult afterRemove4(KvResult kvResult) {
                return put(ver++, "d1.dd2", "d1.dd2_v").await(this::afterPut3);
            }

            private FrameCallResult afterPut3(KvResult kvResult) {
                root_3 = backupIndexAndTime("");
                d1_3 = backupIndexAndTime("d1");
                k1_3 = backupIndexAndTime("k1");
                d1dd2_3 = backupIndexAndTime("d1.dd2");
                return takeSnapshot(this::afterTakeSnapshotS3);
            }

            private FrameCallResult afterTakeSnapshotS3(Snapshot snapshot) {
                s3 = (KvSnapshot) snapshot;
                return Fiber.frameReturn();
            }
        });

        {
            DtKV newKv = copyTo(s1);
            assertEquals("k1_v", getStr(newKv, "k1"));
            assertEquals("k2_v", getStr(newKv, "k2"));
            assertEquals("d1.k1_v", getStr(newKv, "d1.k1"));
            assertEquals("d1.k2_v", getStr(newKv, "d1.k2"));
            assertEquals("d1.dd1.k1_v", getStr(newKv, "d1.dd1.k1"));
            assertEquals("d1.dd1.k2_v", getStr(newKv, "d1.dd1.k2"));
            assertEquals("d1.dd1.ddd1.k1_v", getStr(newKv, "d1.dd1.ddd1.k1"));
            assertEquals("d1.dd1.ddd1.k2_v", getStr(newKv, "d1.dd1.ddd1.k2"));
            assertEquals("d1.dd2.k1_v", getStr(newKv, "d1.dd2.k1"));
            assertEquals("d1.dd2.k2_v", getStr(newKv, "d1.dd2.k2"));
            for (int i = 0; i < 50; i++) {
                assertEquals("value" + i, getStr(newKv, "key" + i));
            }
            checkIndexAndTime(newKv, "", root_1);
            checkIndexAndTime(newKv, "d1", d1_1);
            checkIndexAndTime(newKv, "d1.k2", d1k2_1);
            checkIndexAndTime(newKv, "d1.dd1", d1dd1_1);
            stop(newKv);
        }
        {
            DtKV newKv = copyTo(s2);
            assertEquals("d1.k2_v2", getStr(newKv, "d1.k2"));

            assertEquals("k1_v", getStr(newKv, "k1"));
            assertEquals("k2_v", getStr(newKv, "k2"));
            assertEquals("d1.k1_v", getStr(newKv, "d1.k1"));
            assertEquals("d1.dd1.k1_v", getStr(newKv, "d1.dd1.k1"));
            assertEquals("d1.dd1.k2_v", getStr(newKv, "d1.dd1.k2"));
            assertEquals("d1.dd1.ddd1.k1_v", getStr(newKv, "d1.dd1.ddd1.k1"));
            assertEquals("d1.dd1.ddd1.k2_v", getStr(newKv, "d1.dd1.ddd1.k2"));
            assertEquals("d1.dd2.k1_v", getStr(newKv, "d1.dd2.k1"));
            assertEquals("d1.dd2.k2_v", getStr(newKv, "d1.dd2.k2"));
            for (int i = 0; i < 50; i++) {
                assertEquals("value" + i, getStr(newKv, "key" + i));
            }
            checkIndexAndTime(newKv, "", root_2);
            checkIndexAndTime(newKv, "d1", d1_2);
            checkIndexAndTime(newKv, "d1.k2", d1k2_2);
            checkIndexAndTime(newKv, "k1", k1_2);
            stop(newKv);
        }
        {
            DtKV newKv = copyTo(s3);
            assertTrue(get(newKv, "k1").getNode().isDir);
            assertEquals("k1.k1_v", getStr(newKv, "k1.k1"));
            assertEquals(KvCodes.CODE_NOT_FOUND, get(newKv, "d1.dd2.k1").getBizCode());
            assertEquals(KvCodes.CODE_NOT_FOUND, get(newKv, "d1.dd2.k2").getBizCode());
            assertEquals("d1.dd2_v", getStr(newKv, "d1.dd2"));

            assertEquals("d1.k2_v2", getStr(newKv, "d1.k2"));

            assertEquals("k2_v", getStr(newKv, "k2"));
            assertEquals("d1.k1_v", getStr(newKv, "d1.k1"));
            assertEquals("d1.dd1.k1_v", getStr(newKv, "d1.dd1.k1"));
            assertEquals("d1.dd1.k2_v", getStr(newKv, "d1.dd1.k2"));
            assertEquals("d1.dd1.ddd1.k1_v", getStr(newKv, "d1.dd1.ddd1.k1"));
            assertEquals("d1.dd1.ddd1.k2_v", getStr(newKv, "d1.dd1.ddd1.k2"));
            for (int i = 0; i < 50; i++) {
                assertEquals("value" + i, getStr(newKv, "key" + i));
            }
            checkIndexAndTime(newKv, "", root_3);
            checkIndexAndTime(newKv, "d1", d1_3);
            checkIndexAndTime(newKv, "k1", k1_3);
            checkIndexAndTime(newKv, "d1.dd2", d1dd2_3);
            stop(newKv);
        }
        doInFiber(() -> {
            s1.close();
            s2.close();
            s3.close();
        });
        {
            DtKV newKv = createAndStart();
            doInFiber(new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    SnapshotInfo si = new SnapshotInfo(0, 0, null, null, null, null, 0);
                    FiberFuture<Snapshot> f = newKv.takeSnapshot(si);
                    return f.await(this::afterTakeSnapshot);
                }

                private FrameCallResult afterTakeSnapshot(Snapshot s) {
                    return Fiber.call(new CopyFrame((KvSnapshot) s, kv), this::justReturn);
                }
            });
            // only root dir
            assertEquals(1, kv.kvStatus.kvImpl.map.size());
            stop(newKv);
        }
        stop(kv);
    }
}

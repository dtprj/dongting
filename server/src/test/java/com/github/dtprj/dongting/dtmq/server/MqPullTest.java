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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtmq.proto.DtMq;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.FutureFrame;
import com.github.dtprj.dongting.raft.RaftRole;
import com.github.dtprj.dongting.raft.impl.GroupComponents;
import com.github.dtprj.dongting.raft.impl.InitFiberFrame;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.store.DefaultRaftLog;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.test.TestDir;
import com.github.dtprj.dongting.test.WaitUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

import static com.github.dtprj.dongting.raft.impl.RaftTaskTest.buildTestReqData;
import static com.github.dtprj.dongting.raft.impl.RaftTaskTest.buildTestReqDataDirect;
import static com.github.dtprj.dongting.test.Tick.tick;
import static org.junit.jupiter.api.Assertions.*;

class MqPullTest extends BaseFiberTest {
    private final List<RaftTask> items = new ArrayList<>();
    private RaftGroupConfigEx config;
    private RaftStatusImpl rs;
    private StatusManager statusManager;
    private DefaultRaftLog raftLog;
    private DtMQ mq;
    private int logReadCount;
    private Runnable beforeLogRead;

    @BeforeEach
    void setup() throws Exception {
        config = new RaftGroupConfigEx(1, "1", "");
        config.dataDir = TestDir.testDir(MqPullTest.class.getSimpleName());
        config.fiberGroup = fiberGroup;
        config.blockIoExecutor = MockExecutors.ioExecutor();
        rs = new RaftStatusImpl(1, dispatcher.ts);
        config.raftStatus = rs;
        config.ts = rs.ts;
        rs.tailCache = new TailCache(config, rs);
        config.logFileSize = 16384;
        config.idxItemsPerFile = 512;
        config.idxCacheSize = 512;
        config.idxFlushThreshold = 256;
        config.mqIdxItemsPerFile = 256;
        config.mqIdxCacheBlocks = 8;
        config.mqIdxFlushThreshold = Integer.MAX_VALUE;
        config.mqIdxFlushBatchItems = 128;
        config.mqIdxFlushIntervalMillis = tick(60_000);
        statusManager = new StatusManager(config);
        raftLog = new DefaultRaftLog(config, statusManager, null);
        mq = new DtMQ(config, new MqServerConfig());
        GroupComponents components = new GroupComponents();
        components.groupConfig = config;
        components.raftStatus = rs;
        components.fiberGroup = fiberGroup;
        components.stateMachine = mq;
        components.raftLog = (RaftLog) Proxy.newProxyInstance(RaftLog.class.getClassLoader(),
                new Class<?>[]{RaftLog.class}, (proxy, method, args) -> {
                    if (method.getName().equals("read")) {
                        logReadCount += (int) args[3];
                        if (beforeLogRead != null) {
                            beforeLogRead.run();
                        }
                    }
                    try {
                        return method.invoke(raftLog, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        mq.setRaftGroup(new RaftGroupImpl(components));
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(statusManager.initStatusFile(), this::initLog);
            }

            private FrameCallResult initLog(Void unused) {
                InitFiberFrame.initRaftStatus(rs, fiberGroup, new RaftServerConfig());
                return Fiber.call(raftLog.init(), result -> {
                    leader();
                    return mq.start().await(this::justReturn);
                });
            }
        });
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            await(mq::stop);
            await(() -> FiberFuture.allOf("closePullTest", raftLog.close(), statusManager.close(true)));
        } finally {
            doInFiber(() -> rs.tailCache.cleanAll());
            items.forEach(item -> item.reqData.release());
        }
    }

    private void leader() {
        rs.setRole(RaftRole.leader);
        rs.setGroupReady(true);
        rs.setLeaseStartNanos(System.nanoTime());
        rs.setElectTimeoutNanos(tick(600_000L) * 1_000_000);
        rs.copyShareStatus();
    }

    private void await(Supplier<FiberFuture<Void>> action) throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return action.get().await(this::justReturn);
            }
        });
    }

    private RaftTask item(long queueId, int totalLen) {
        byte[] header = {1, 2, 3, 4};
        byte[] body = new byte[totalLen - LogHeader.computeTotalLen(header.length, 0) - 4];
        Arrays.fill(body, (byte) (items.size() + 1));
        RaftReqData rd = totalLen >= RaftServerConfig.GATHERING_WRITE_THRESHOLD
                ? buildTestReqDataDirect(LogHeader.TYPE_NORMAL, 2, header, body)
                : buildTestReqData(LogHeader.TYPE_NORMAL, 2, header, body);
        rd.index = items.size() + 1;
        rd.term = 1;
        rd.prevLogTerm = rd.index == 1 ? 0 : 1;
        rd.bizKey = queueId;
        LogHeader.writeAndComputeCrc(rd, new CRC32C(), rd.buffer.getBuffer(), 0);
        RaftTask task = new RaftTask(rd, null, null, false);
        items.add(task);
        return task;
    }

    private void append(RaftTask... batch) throws Exception {
        long lastIndex = batch[batch.length - 1].reqData.index;
        doInFiber(new FiberFrame<>() {
            private int next;

            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(raftLog.append(Arrays.asList(batch)), this::waitForce);
            }

            private FrameCallResult waitForce(Void unused) {
                if (rs.lastForceLogIndex < lastIndex) {
                    return rs.logForceFinishCondition.await(tick(1000), this::waitForce);
                }
                return apply(null);
            }

            private FrameCallResult apply(Object unused) {
                if (next < batch.length) {
                    RaftTask task = batch[next++];
                    task.reqData.retain();
                    rs.tailCache.put(task.reqData.index, task);
                    rs.tailCache.removePending(task);
                    return mq.exec(task).await(this::apply);
                }
                rs.lastLogIndex = lastIndex;
                rs.commitIndex = lastIndex;
                rs.setLastApplied(lastIndex);
                return Fiber.frameReturn();
            }
        });
    }

    private static final class Pull {
        final MqPullFrame frame;
        final CompletableFuture<MqPullResp> result = new CompletableFuture<>();

        Pull(MqPullFrame frame) {
            this.frame = frame;
            FutureFrame.startWaitFiber("testPull", fiberGroup, frame).registerCallback((buf, ex) -> {
                if (ex == null) {
                    result.complete(buf);
                } else {
                    result.completeExceptionally(ex);
                }
            });
        }
    }

    private Pull pull(long[] ids, long[] seqs, int limit, int bytesLimit, long waitMillis) throws Exception {
        AtomicReference<Pull> result = new AtomicReference<>();
        doInFiber(() -> result.set(new Pull(mq.pull(ids, seqs, limit, bytesLimit, waitMillis,
                new DtTime(tick(30_000), TimeUnit.MILLISECONDS)))));
        return result.get();
    }

    private Pull waitingPull(long... ids) throws Exception {
        Pull p = pull(ids, new long[ids.length], 10, 4096, tick(20_000));
        waitForWatch(p, ids);
        return p;
    }

    private void waitForWatch(Pull p, long... ids) {
        WaitUtil.waitUtil(true, () -> {
            for (long id : ids) {
                if (mq.waitingPulls.get(id) == null || !mq.waitingPulls.get(id).contains(p.frame)) {
                    return false;
                }
            }
            return true;
        }, tick(3000), fiberGroup.getExecutor());
        assertFalse(p.result.isDone());
    }

    private void assertRecords(Pull p, RaftTask... expected) throws Exception {
        MqPullResp resp = p.result.get(tick(3000), TimeUnit.MILLISECONDS);
        try {
            ByteBuffer encoded = ByteBuffer.allocate(resp.actualSize());
            ByteBuffer dest = ByteBuffer.allocate(64);
            EncodeContext context = new EncodeContext(null);
            boolean finished;
            do {
                finished = resp.encode(context, dest);
                dest.flip();
                encoded.put(dest);
                dest.clear();
                if (resp.hasPreEncodedBuffer()) {
                    assertFalse(finished);
                    ByteBuffer segment = resp.getPreEncodedBuffer();
                    context.pending += segment.remaining();
                    encoded.put(segment);
                }
            } while (!finished);
            assertEquals(resp.actualSize(), encoded.position());
            assertEquals(resp.actualBodySize(), resp.getTotalPreEncodedBufferSize());
            encoded.flip().position(resp.actualSize() - resp.actualBodySize());
            DtMq.MqPullResp decoded = DtMq.MqPullResp.parseFrom(encoded.slice());
            assertEquals(p.frame.queueIds.length, decoded.getResultsSize());
            assertEquals(p.frame.queueIds.length, decoded.getResultsCount());
            for (int i = 0; i < decoded.getResultsCount(); i++) {
                MqPullFrame.QueueResult r = p.frame.results == null ? null : p.frame.results[i];
                DtMq.MqPullQueueResult d = decoded.getResults(i);
                assertEquals(p.frame.queueIds[i], d.getQueueId());
                assertEquals(r == null ? p.frame.fromSeqs[i] : r.fromSeq, d.getFromSeq());
                assertEquals(r == null ? p.frame.fromSeqs[i] : r.nextSeq, d.getNextSeq());
                assertEquals(r == null ? 0 : r.count, d.getCount());
                assertEquals(r != null && r.truncated, d.getTruncated());
                assertEquals(r == null ? 0 : r.requiredBytes, d.getRequiredBytes());
            }
            ByteBuffer actual = decoded.getRecords().asReadOnlyByteBuffer();
            for (RaftTask task : expected) {
                ByteBuffer record = task.reqData.buffer.getBuffer().duplicate();
                record.clear().limit(task.reqData.totalLen);
                ByteBuffer part = actual.slice();
                part.limit(record.remaining());
                assertEquals(record, part, "raw record at raft index " + task.reqData.index);
                actual.position(actual.position() + record.remaining());
            }
            assertFalse(actual.hasRemaining());
        } finally {
            resp.clean();
        }
    }

    private void assertQueue(Pull p, int index, long from, long next, int count,
                             boolean truncated, int requiredBytes) {
        MqPullFrame.QueueResult r = p.frame.results == null ? null : p.frame.results[index];
        if (r == null) {
            assertEquals(p.frame.fromSeqs[index], from);
            assertEquals(p.frame.fromSeqs[index], next);
            assertEquals(0, count);
            assertFalse(truncated);
            assertEquals(0, requiredBytes);
            return;
        }
        assertEquals(from, r.fromSeq);
        assertEquals(next, r.nextSeq);
        assertEquals(count, r.count);
        assertEquals(truncated, r.truncated);
        assertEquals(requiredBytes, r.requiredBytes);
    }

    private void assertFailure(Pull p, Class<? extends Throwable> type) {
        assertThrows(type, () -> {
            try {
                MqPullResp unexpected = p.result.get(tick(3000), TimeUnit.MILLISECONDS);
                if (unexpected != null) {
                    unexpected.clean();
                }
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        });
    }

    private void assertClean() throws Exception {
        doInFiber(() -> {
            assertTrue(mq.activePulls.isEmpty());
            assertEquals(0, mq.waitingPulls.size());
        });
    }

    @Test
    void testCacheBatchAndSharedLimits() throws Exception {
        RaftTask a = item(11, 128);
        RaftTask b = item(22, 160);
        RaftTask c = item(11, 192);
        RaftTask d = item(22, 96);
        append(a, b, c, d);
        long[] ids = {22, 11, 99};
        long[] seqs = {0, 0, 0};
        Pull all = pull(ids, seqs, 10, 4096, 0);
        assertRecords(all, b, d, a, c);
        assertEquals(3, all.frame.queueIds.length);
        assertQueue(all, 0, 0, 2, 2, false, 0);
        assertQueue(all, 1, 0, 2, 2, false, 0);
        assertQueue(all, 2, 0, 0, 0, false, 0);

        Pull countLimit = pull(ids, seqs, 3, 4096, 0);
        assertRecords(countLimit, b, d, a);
        assertQueue(countLimit, 1, 0, 1, 1, false, 0);
        Pull bytesLimit = pull(ids, seqs, 10, 383, 0);
        assertRecords(bytesLimit, b, d);
        assertQueue(bytesLimit, 1, 0, 0, 0, false, 0);

        for (int maxBytes : new int[]{1, 191}) {
            Pull tooLarge = pull(new long[]{11}, new long[]{1}, 10, maxBytes, tick(20_000));
            assertRecords(tooLarge);
            assertQueue(tooLarge, 0, 1, 1, 0, false, 192);
        }
        Pull empty = pull(new long[]{11, 99}, new long[]{2, 17}, 10, 4096, 0);
        assertRecords(empty);
        assertQueue(empty, 0, 2, 2, 0, false, 0);
        assertQueue(empty, 1, 17, 17, 0, false, 0);
        assertEquals(0, logReadCount);
        assertClean();
    }

    @Test
    void testMemoryFirstWithSharedLimits() throws Exception {
        RaftTask cold = item(1, 128);
        append(cold, item(1, 160));
        doInFiber(() -> rs.tailCache.cleanAll());
        RaftTask small = item(2, 96);
        RaftTask large = item(2, RaftServerConfig.GATHERING_WRITE_THRESHOLD);
        append(small, large);
        long[] ids = {1, 2};
        long[] seqs = {0, 0};

        Pull countLimit = pull(ids, seqs, 2, 65536, 0);
        assertRecords(countLimit, small, large);
        assertQueue(countLimit, 0, 0, 0, 0, false, 0);
        assertQueue(countLimit, 1, 0, 2, 2, false, 0);
        Pull bytesLimit = pull(ids, seqs, 10, small.reqData.totalLen + large.reqData.totalLen, 0);
        assertRecords(bytesLimit, small, large);
        assertQueue(bytesLimit, 0, 0, 0, 0, false, 0);
        assertQueue(bytesLimit, 1, 0, 2, 2, false, 0);
        assertEquals(0, logReadCount);

        Pull remaining = pull(ids, seqs, 3, 65536, 0);
        assertRecords(remaining, cold, small, large);
        assertQueue(remaining, 0, 0, 1, 1, false, 0);
        assertQueue(remaining, 1, 0, 2, 2, false, 0);
        assertEquals(1, logReadCount);
        assertClean();
    }

    @Test
    void testMixedCacheAndDiskWithEviction() throws Exception {
        RaftTask a = item(1, 128);
        RaftTask b = item(2, 160);
        append(a, b);
        doInFiber(() -> rs.tailCache.cleanAll());
        int threshold = RaftServerConfig.GATHERING_WRITE_THRESHOLD;
        RaftTask small = item(1, threshold - 1);
        RaftTask large = item(1, threshold);
        RaftTask last = item(2, threshold + 1);
        append(small, large, last);
        beforeLogRead = () -> {
            rs.tailCache.cleanAll();
            for (RaftTask task : new RaftTask[]{large, last}) {
                task.reqData.release();
                items.remove(task);
                assertTrue(task.reqData.isReleased());
                assertFalse(task.reqData.buffer.isReleased());
            }
        };
        ByteBuffer source = large.reqData.buffer.getBuffer();
        source.position(3).limit(7);
        Pull mixed = pull(new long[]{1, 2}, new long[]{0, 0}, 10, 65536, 0);
        assertRecords(mixed, a, small, large, b, last);
        assertEquals(2, logReadCount);
        assertEquals(3, source.position());
        assertEquals(7, source.limit());
        assertTrue(large.reqData.buffer.isReleased());
        assertTrue(last.reqData.buffer.isReleased());
        assertClean();
    }

    @Test
    void testCachedResponseCleanupWithoutSend() throws Exception {
        RaftTask task = item(1, RaftServerConfig.GATHERING_WRITE_THRESHOLD);
        append(task);
        Pull cached = pull(new long[]{1}, new long[]{0}, 1, 65536, 0);
        MqPullResp resp = cached.result.get(tick(3000), TimeUnit.MILLISECONDS);
        doInFiber(() -> {
            rs.tailCache.cleanAll();
            task.reqData.release();
            items.remove(task);
            assertFalse(task.reqData.buffer.isReleased());
        });
        resp.clean();
        assertTrue(task.reqData.buffer.isReleased());
        assertEquals(0, logReadCount);
        assertClean();
    }

    @Test
    void testMixedReadCancellation() throws Exception {
        append(item(1, 128));
        doInFiber(() -> rs.tailCache.cleanAll());
        RaftTask task = item(1, RaftServerConfig.GATHERING_WRITE_THRESHOLD);
        append(task);
        beforeLogRead = () -> {
            mq.cancelPendingPulls();
            rs.tailCache.cleanAll();
            task.reqData.release();
            items.remove(task);
            assertFalse(task.reqData.buffer.isReleased());
        };
        Pull cancelled = pull(new long[]{1}, new long[]{0}, 10, 65536, 0);
        assertFailure(cancelled, CancellationException.class);
        assertEquals(1, logReadCount);
        assertTrue(task.reqData.buffer.isReleased());
        assertClean();
    }

    @Test
    void testDiskIndexesAfterEvictionAndRestore() throws Exception {
        RaftTask[] batch = new RaftTask[260];
        Arrays.setAll(batch, i -> item(1, 64));
        append(batch);
        doInFiber(() -> mq.manager.register(2, 130));
        RaftTask restored = item(2, 64);
        append(restored);
        await(() -> mq.manager.flusher.flushAll());
        doInFiber(() -> {
            assertNotNull(mq.manager.remove());
            assertNotNull(mq.manager.remove());
            assertNull(mq.manager.remove());
            assertEquals(-1, mq.manager.getIdxItemInCache(1, 126));
        });
        RaftTask hot = item(3, 64);
        append(hot);
        Pull memory = pull(new long[]{1, 3}, new long[]{126, 0}, 1, 4096, 0);
        assertRecords(memory, hot);
        assertQueue(memory, 0, 126, 126, 0, false, 0);
        assertQueue(memory, 1, 0, 1, 1, false, 0);
        assertEquals(0, logReadCount);
        Pull evicted = pull(new long[]{1, 3}, new long[]{126, 0}, 6, 4096, 0);
        assertRecords(evicted, batch[126], batch[127], batch[128], batch[129], batch[130], hot);
        assertQueue(evicted, 0, 126, 131, 5, false, 0);
        assertQueue(evicted, 1, 0, 1, 1, false, 0);

        await(() -> mq.startInstall(false));
        doInFiber(() -> {
            mq.manager.register(1, 260);
            mq.manager.register(2, 131);
            assertEquals(0, mq.manager.queues.get(1).blocks.size());
            assertEquals(0, mq.manager.queues.get(2).blocks.size());
        });
        await(() -> mq.installSnapshot(0, 0, 0, true, null));
        Pull disk = pull(new long[]{1}, new long[]{254}, 10, 4096, 0);
        assertRecords(disk, Arrays.copyOfRange(batch, 254, 260));
        assertQueue(disk, 0, 254, 260, 6, false, 0);

        doInFiber(() -> rs.firstValidPos = config.logFileSize);
        Pull old = pull(new long[]{1, 2}, new long[]{1, 129}, 10, 4096, tick(20_000));
        assertRecords(old);
        assertQueue(old, 0, 256, 256, 0, true, 0);
        assertQueue(old, 1, 130, 130, 0, true, 0);
        Pull zero = pull(new long[]{2, 1}, new long[]{0, 0}, 3, 4096, 0);
        assertRecords(zero, restored, batch[256], batch[257]);
        assertQueue(zero, 0, 130, 131, 1, false, 0);
        assertQueue(zero, 1, 256, 258, 2, false, 0);
        assertClean();
    }

    @Test
    void testLongPollNewQueueAndTimeout() throws Exception {
        Pull waiting = waitingPull(99, 100);
        doInFiber(() -> assertNull(mq.manager.queues.get(99)));
        RaftTask task = item(99, 128);
        append(task);
        assertRecords(waiting, task);
        assertQueue(waiting, 0, 0, 1, 1, false, 0);
        assertQueue(waiting, 1, 0, 0, 0, false, 0);
        assertClean();

        long start = System.nanoTime();
        Pull timeout = pull(new long[]{99, 100}, new long[]{1, 0}, 10, 4096, tick(30));
        assertRecords(timeout);
        assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(tick(30)));
        assertQueue(timeout, 0, 1, 1, 0, false, 0);
        assertClean();
    }

    @Test
    void testCancellationAndLifecycleCleanup() throws Exception {
        Pull first = waitingPull(1, 2);
        Pull second = waitingPull(2, 3);
        doInFiber(first.frame::cancel);
        assertFailure(first, CancellationException.class);
        waitForWatch(second, 2, 3);
        doInFiber(() -> {
            assertNull(mq.waitingPulls.get(1));
            assertEquals(1, mq.activePulls.size());
            assertEquals(1, mq.waitingPulls.get(2).size());
            mq.cancelPendingPulls();
        });
        assertFailure(second, CancellationException.class);
        assertClean();

        Pull install = waitingPull(1, 2);
        await(() -> mq.startInstall(false));
        assertFailure(install, CancellationException.class);
        assertClean();
        await(() -> mq.installSnapshot(0, 0, 0, true, null));
        Pull stop = waitingPull(2, 3);
        await(mq::stop);
        assertFailure(stop, CancellationException.class);
        assertClean();
    }
}

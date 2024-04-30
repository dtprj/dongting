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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.buf.TwoLevelPool;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.impl.InitFiberFrame;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.TailCache;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftServerConfig;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import com.github.dtprj.dongting.raft.test.TestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * @author huangli
 */
public class DefaultRaftLogTest extends BaseFiberTest {
    private String dataDir;
    private RaftStatusImpl raftStatus;
    private RaftGroupConfigEx config;
    private StatusManager statusManager;
    private DefaultRaftLog raftLog;

    @BeforeEach
    public void setup() throws Exception {
        dataDir = TestDir.testDir(DefaultRaftLogTest.class.getSimpleName());
        init();
    }

    private void init() throws Exception {
        raftStatus = new RaftStatusImpl(dispatcher.getTs());
        RaftServerConfig serverConfig = new RaftServerConfig();
        config = new RaftGroupConfigEx(1, "1", "1");
        config.setFiberGroup(fiberGroup);
        config.setDataDir(dataDir);
        config.setIoExecutor(MockExecutors.ioExecutor());
        config.setTs(raftStatus.getTs());
        config.setDirectPool(TwoLevelPool.getDefaultFactory().apply(config.getTs(), true));
        config.setHeapPool(new RefBufferFactory(TwoLevelPool.getDefaultFactory().apply(config.getTs(), false), 0));

        raftStatus.setTailCache(new TailCache(config, raftStatus));
        config.setRaftStatus(raftStatus);
        statusManager = new StatusManager(config);
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(statusManager.initStatusFile(), this::resume);
            }

            private FrameCallResult resume(Void v) {
                return Fiber.frameReturn();
            }
        });

        raftLog = new DefaultRaftLog(config, statusManager, null,1);
        raftLog.idxItemsPerFile = 8;
        raftLog.idxMaxCacheItems = 4;
        raftLog.logFileSize = 1024;
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                InitFiberFrame.initRaftStatus(raftStatus, fiberGroup, serverConfig);
                return Fiber.call(raftLog.init(), this::resume);
            }

            private FrameCallResult resume(Pair<Integer, Long> integerLongPair) {
                return Fiber.frameReturn();
            }
        });
    }

    @AfterEach
    public void tearDown() throws Exception {
        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                FiberFuture<Void> f1 = raftLog.close();
                FiberFuture<Void> f2 = statusManager.close();
                return FiberFuture.allOf("close", f1, f2).await(this::justReturn);
            }
        });
    }

    private void append(int index, int[] totalSizes, int[] bizHeaderLen) throws Exception {
        LogItem[] items = new LogItem[totalSizes.length];
        for (int i = 0; i < totalSizes.length; i++) {
            items[i] = LogFileQueueTest.createItem(config, 100, 100, index++, totalSizes[i], bizHeaderLen[i]);
            RaftInput ri = new RaftInput(0, null, null, null);
            RaftTask rt = new RaftTask(config.getTs(), LogItem.TYPE_NORMAL, ri, null);
            rt.setItem(items[i]);
            raftStatus.getTailCache().put(items[i].getIndex(), rt);
        }

        doInFiber(new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                raftStatus.getDataArrivedCondition().signalAll();
                return waitWriteFinish(null);
            }

            private FrameCallResult waitWriteFinish(Void v) {
                if (raftLog.logFiles.logAppender.writeNotFinish()) {
                    return raftStatus.getLogForceFinishCondition().await(1000, this::waitWriteFinish);
                } else {
                    return Fiber.frameReturn();
                }
            }
        });
    }

    @Test
    public void testInit() throws Exception {
        int[] totalSizes = new int[]{400, 400, 512};
        int[] bizHeaderLen = new int[]{1, 0, 400};
        append(1, totalSizes, bizHeaderLen);
        tearDown();
        init();
    }

    @Test
    public void testDelete() throws Exception {
        int[] totalSizes = new int[]{400, 400, 512, 200, 400};
        int[] bizHeaderLen = new int[]{1, 0, 400, 100, 1};
        append( 1, totalSizes, bizHeaderLen);
        raftStatus.setCommitIndex(5);
        raftStatus.setLastApplied(5);
        raftStatus.setLastLogIndex(5);
        append(6, totalSizes, bizHeaderLen);

        // test delete
        File dir = new File(new File(dataDir), "log");

        {
            Supplier<Boolean> deleted = fileDeleted(dir, 0);
            doInFiber(() -> raftLog.markTruncateByIndex(3, 1000));
            Thread.sleep(2);
            assertFalse(deleted.get());
            plus1Hour();
            TestUtil.waitUtil(deleted);
        }
        {
            plus1Hour();
            doInFiber(() -> raftLog.markTruncateByTimestamp(raftStatus.getTs().getWallClockMillis(), 0));

            // can't delete after next persist index and apply index, so only delete to index 4
            Supplier<Boolean> deleted = fileDeleted(dir, 1024);
            plus1Hour();
            TestUtil.waitUtil(deleted);
        }
    }

    private static Supplier<Boolean> fileDeleted(File dir, long startPos) {
        return () -> {
            String[] names = dir.list();
            String firstFileName = String.format("%020d", startPos);
            if (names != null) {
                for (String n : names) {
                    if (n.equals(firstFileName)) {
                        return false;
                    }
                }
            }
            return true;
        };
    }

    private void plus1Hour() throws Exception {
        doInFiber(() -> TestUtil.plus1Hour(raftStatus.getTs()));
    }
}

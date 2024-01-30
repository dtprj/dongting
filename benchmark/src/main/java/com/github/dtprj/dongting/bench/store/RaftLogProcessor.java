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
package com.github.dtprj.dongting.bench.store;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.buf.TwoLevelPool;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.codec.RefBufferDecoder;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.IndexedQueue;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.fiber.Dispatcher;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.RefBufWriteFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.InitFiberFrame;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;
import com.github.dtprj.dongting.raft.store.ByteBufferEncoder;
import com.github.dtprj.dongting.raft.store.DefaultRaftLog;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class RaftLogProcessor extends ReqProcessor<RefBuffer> {

    public static final int COMMAND = 10001;
    private static final String DATA_DIR = "target/raftlog";

    private final FiberChannel<ReqData> queue;

    private final RaftLog raftLog;
    private final StatusManager statusManager;
    private final RaftStatusImpl raftStatus;
    private final Timestamp ts;
    private long nextIndex = 1;

    private final Dispatcher dispatcher;
    private final FiberGroup fiberGroup;
    private final IndexedQueue<ReqData> pending = new IndexedQueue<>(16 * 1024);

    static class ReqData {
        ReadFrame<RefBuffer> frame;
        ChannelContext channelContext;
        ReqContext reqContext;
        long raftLogIndex;
    }

    public RaftLogProcessor() {
        dispatcher = new Dispatcher("RaftLogDispatcher");
        dispatcher.start();
        fiberGroup = new FiberGroup("bench test group", dispatcher);
        dispatcher.startGroup(fiberGroup).join();
        queue = fiberGroup.newChannel();
        raftStatus = new RaftStatusImpl(dispatcher.getTs());
        this.ts = dispatcher.getTs();
        RaftGroupConfigEx groupConfig = new RaftGroupConfigEx(1, "1", "1");
        groupConfig.setFiberGroup(fiberGroup);
        groupConfig.setDataDir(DATA_DIR);
        groupConfig.setIoExecutor(MockExecutors.ioExecutor());
        groupConfig.setTs(dispatcher.getTs());
        groupConfig.setDirectPool(TwoLevelPool.getDefaultFactory().apply(groupConfig.getTs(), true));
        groupConfig.setHeapPool(new RefBufferFactory(TwoLevelPool.getDefaultFactory().apply(groupConfig.getTs(), false), 128));
        groupConfig.setRaftStatus(raftStatus);
        groupConfig.setCodecFactory(new RaftCodecFactory() {
            @Override
            public Decoder<?> createHeaderDecoder(int bizType) {
                return null;
            }

            @Override
            public Decoder<?> createBodyDecoder(int bizType) {
                return null;
            }

            @Override
            public Encoder<?> createHeaderEncoder(int bizType) {
                return null;
            }

            private final Encoder<RefBuffer> encoder = new Encoder<>() {
                @Override
                public boolean encode(EncodeContext context, ByteBuffer buffer, RefBuffer data) {
                    return ByteBufferEncoder.INSTANCE.encode(context, buffer, data.getBuffer());
                }

                @Override
                public int actualSize(RefBuffer data) {
                    return data.getBuffer().remaining();
                }
            };

            @Override
            public Encoder<?> createBodyEncoder(int bizType) {
                return encoder;
            }
        });

        statusManager = new StatusManager(groupConfig);
        this.raftLog = new DefaultRaftLog(groupConfig, statusManager);
        CompletableFuture<Void> initFuture = new CompletableFuture<>();

        fiberGroup.fireFiber("init", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                InitFiberFrame.initRaftStatus(raftStatus, fiberGroup);
                return Fiber.call(statusManager.initStatusFile(), this::afterStatusManagerInit);
            }

            private FrameCallResult afterStatusManagerInit(Void unused) throws Exception {
                return Fiber.call(raftLog.init(), this::afterRaftLogInit);
            }

            private FrameCallResult afterRaftLogInit(Pair<Integer, Long> p) {
                nextIndex = p.getRight() + 1;
                initFuture.complete(null);
                return Fiber.frameReturn();
            }
        });
        Fiber storeFiber = new Fiber("store-fiber", fiberGroup, new StoreFiberFrame(), true);
        fiberGroup.fireFiber(storeFiber);
        Fiber postStoreFiber = new Fiber("post-store-fiber", fiberGroup, new PostStoreFrame(), true);
        fiberGroup.fireFiber(postStoreFiber);
        initFuture.join();
    }

    @Override
    public WriteFrame process(ReadFrame<RefBuffer> frame, ChannelContext channelContext, ReqContext reqContext) {
        ReqData rd = new ReqData();
        rd.frame = frame;
        rd.channelContext = channelContext;
        rd.reqContext = reqContext;
        queue.fireOffer(rd);
        return null;
    }

    @Override
    public Decoder<RefBuffer> createDecoder(int command) {
        return RefBufferDecoder.INSTANCE;
    }

    public static LogItem createItems(long index, Timestamp ts, RefBuffer bodyBuffer) {
        LogItem item = new LogItem(null);
        item.setIndex(index);
        item.setType(1);
        item.setBizType(2);
        item.setTerm(100);
        item.setPrevLogTerm(100);
        item.setTimestamp(ts.getWallClockMillis());

        item.setBody(bodyBuffer);
        item.setActualBodySize(bodyBuffer.getBuffer().remaining());
        return item;
    }

    private class StoreFiberFrame extends FiberFrame<Void> {
        private final ArrayList<ReqData> list = new ArrayList<>(2048);

        @Override
        public FrameCallResult execute(Void input) {
            return queue.takeAll(list, this::afterTake);
        }

        private FrameCallResult afterTake(Void v) {
            for (ReqData reqData : list) {
                LogItem item = createItems(nextIndex++, ts, reqData.frame.getBody());
                RaftInput ri = new RaftInput(0, null, null, null, 0);
                RaftTask rt = new RaftTask(ts, LogItem.TYPE_NORMAL, ri, null);
                rt.setItem(item);
                raftStatus.getTailCache().put(item.getIndex(), rt);

                reqData.raftLogIndex = item.getIndex();
                pending.addLast(reqData);
            }
            list.clear();
            raftStatus.getDataArrivedCondition().signalAll();
            return Fiber.resume(null, this);
        }
    }

    private class PostStoreFrame extends FiberFrame<Void> {

        @Override
        public FrameCallResult execute(Void input) {

            long lastIndex = raftStatus.getLastSyncLogIndex();
            raftStatus.setCommitIndex(lastIndex);
            raftStatus.setLastApplied(lastIndex);
            raftStatus.setLastLogIndex(lastIndex);

            while (pending.size() > 0) {
                ReqData rd = pending.get(0);
                if (rd.raftLogIndex <= lastIndex) {
                    RefBufWriteFrame resp = new RefBufWriteFrame(rd.frame.getBody());
                    resp.setRespCode(CmdCodes.SUCCESS);
                    rd.channelContext.getRespWriter().writeRespInBizThreads(rd.frame, resp, rd.reqContext.getTimeout());
                    pending.removeFirst();
                } else {
                    break;
                }
            }
            return raftStatus.getLogSyncFinishCondition().await(1000, this);
        }
    }

    public void shutdown() {
        fiberGroup.fireFiber("shutdown", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                raftLog.close();
                statusManager.close();
                return Fiber.frameReturn();
            }
        });
        dispatcher.stop(new DtTime(1, TimeUnit.SECONDS));
    }

}

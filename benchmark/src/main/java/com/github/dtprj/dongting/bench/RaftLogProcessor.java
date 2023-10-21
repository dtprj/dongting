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
package com.github.dtprj.dongting.bench;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.buf.TwoLevelPool;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.RefBufferDecoder;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespFrame;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.RefBufWriteFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.store.DefaultRaftLog;
import com.github.dtprj.dongting.raft.store.RaftLog;
import com.github.dtprj.dongting.raft.store.StatusManager;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/**
 * @author huangli
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class RaftLogProcessor extends ReqProcessor<RefBuffer> {

    public static final int COMMAND = 10001;
    private static final String DATA_DIR = "target/raftlog";

    private final LinkedBlockingQueue<ReqData> queue = new LinkedBlockingQueue<>();

    private final RaftLog raftLog;
    private final RaftStatusImpl raftStatus;
    private final Timestamp ts;
    private final Supplier<Boolean> stopIndicator;
    private long nextIndex;

    private final ArrayList<ReqData> list = new ArrayList<>(5000);
    private long processItemCount = 0;
    private long processBatchCount = 0;

    static class ReqData {
        ReadFrame<RefBuffer> frame;
        ChannelContext channelContext;
        ReqContext reqContext;
    }

    public RaftLogProcessor(Supplier<Boolean> stopIndicator) {
        this.stopIndicator = stopIndicator;
        try {
            raftStatus = new RaftStatusImpl();
            this.ts = raftStatus.getTs();
            RaftGroupConfig config = new RaftGroupConfig(1, "1", "1");
            config.setDataDir(DATA_DIR);
            config.setRaftExecutor(MockExecutors.raftExecutor());
            config.setIoExecutor(MockExecutors.ioExecutor());
            config.setStopIndicator(stopIndicator);
            config.setTs(raftStatus.getTs());
            config.setDirectPool(TwoLevelPool.getDefaultFactory().apply(config.getTs(), true));
            config.setHeapPool(new RefBufferFactory(TwoLevelPool.getDefaultFactory().apply(config.getTs(), false), 128));
            // TODO just fix compile
            StatusManager statusManager = new StatusManager(config, raftStatus);
            statusManager.initStatusFile();
            config.setRaftStatus(raftStatus);
            config.setIoExecutor(MockExecutors.ioExecutor());

            this.raftLog = new DefaultRaftLog(config, statusManager);
            // TODO just fix compile
            Pair<Integer, Long> initResult = this.raftLog.init(null);
            nextIndex = initResult.getRight() + 1;

            MockExecutors.raftExecutor().execute(this::run);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public WriteFrame process(ReadFrame<RefBuffer> frame, ChannelContext channelContext, ReqContext reqContext) {
        ReqData rd = new ReqData();
        rd.frame = frame;
        rd.channelContext = channelContext;
        rd.reqContext = reqContext;
        queue.add(rd);
        return null;
    }

    @Override
    public Decoder<RefBuffer> createDecoder() {
        return RefBufferDecoder.PLAIN_INSTANCE;
    }

    public static LogItem createItems(long index, Timestamp ts, RefBuffer bodyBuffer) {
        LogItem item = new LogItem(null);
        item.setIndex(index);
        item.setType(1);
        item.setBizType(2);
        item.setTerm(100);
        item.setPrevLogTerm(100);
        item.setTimestamp(ts.getWallClockMillis());

        item.setBodyBuffer(bodyBuffer.getBuffer());
        item.setActualBodySize(bodyBuffer.getBuffer().remaining());
        return item;
    }

    private void run() {
        if (stopIndicator.get()) {
            System.out.println("raft log append avg processBatchCount: " + (1.0 * processItemCount / processBatchCount));
            return;
        }
        try {
            ts.refresh(1);
            queue.drainTo(list);
            if (list.size() > 0) {
                try {
                    processBatchCount++;
                    ArrayList<LogItem> items = new ArrayList<>(list.size());
                    for (int i = 0; i < list.size(); i++) {
                        processItemCount++;
                        ReqData rd = list.get(i);
                        RefBuffer bodyBuffer = rd.frame.getBody();
                        LogItem item = createItems(nextIndex + i, ts, bodyBuffer);
                        items.add(item);
                    }
                    // TODO just fix compile
                    raftLog.append();
                    nextIndex += list.size();
                    raftStatus.setCommitIndex(nextIndex - 1);
                    raftStatus.setLastApplied(nextIndex - 1);
                    raftStatus.setLastLogIndex(nextIndex - 1);
                    for (int i = 0; i < list.size(); i++) {
                        ReqData rd = list.get(i);
                        RefBufWriteFrame resp = new RefBufWriteFrame(rd.frame.getBody());
                        resp.setRespCode(CmdCodes.SUCCESS);
                        rd.channelContext.getRespWriter().writeRespInBizThreads(rd.frame, resp, rd.reqContext.getTimeout());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    for (int i = 0; i < list.size(); i++) {
                        ReqData rd = list.get(i);
                        EmptyBodyRespFrame resp = new EmptyBodyRespFrame(CmdCodes.SYS_ERROR);
                        rd.channelContext.getRespWriter().writeRespInBizThreads(rd.frame, resp, rd.reqContext.getTimeout());
                    }
                }
                list.clear();
            }
            MockExecutors.raftExecutor().execute(this::run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

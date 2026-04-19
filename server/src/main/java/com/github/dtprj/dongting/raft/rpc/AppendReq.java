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
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.common.DtCleanable;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftRpcData;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;
import com.github.dtprj.dongting.raft.store.LogHeader;
import com.github.dtprj.dongting.raft.store.RaftLogData;
import com.github.dtprj.dongting.raft.store.RaftLogDataCallback;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Function;

/**
 * @author huangli
 */
//message AppendEntriesReq {
//int32 group_id = 1;
//int32 term = 2;
//int32 leader_id = 3;
//fixed64 prev_log_index = 4;
//int32 prev_log_term = 5;
//fixed64 leader_commit = 6;
//int32 logs_size = 7;
//bytes entries = 8;
//}
public class AppendReq extends RaftRpcData implements DtCleanable {
    private static final DtLog log = DtLogs.getLogger(AppendReq.class);

    public static final int IDX_GROUP_ID = 1;
    public static final int IDX_TERM = 2;
    public static final int IDX_LEADER_ID = 3;
    public static final int IDX_PREV_LOG_INDEX = 4;
    public static final int IDX_PREV_LOG_TERM = 5;
    public static final int IDX_LEADER_COMMIT = 6;
    public static final int IDX_LOGS_SIZE = 7;
    public static final int IDX_ENTRIES = 8;

    // private int groupId;
    // private int term;
    public int leaderId;
    public long prevLogIndex;
    public int prevLogTerm;
    public long leaderCommit;
    public ArrayList<RaftTask> logs;

    @Override
    public void clean() {
        if (logs != null) {
            RaftUtil.releaseInputs(logs);
            logs = null;
        }
    }

    // re-used
    public static class Callback extends PbCallback<AppendReq> {

        private final RaftLogDataCallback logDataCallback;

        private final Decoder headerBodyDecoder;
        private final DecodeContext headerBodyContext;

        private AppendReq result;

        public Callback(Function<Integer, RaftCodecFactory> decoderFactory,
                        RefBufferFactory heapPool, byte[] threadLocalBuffer) {
            this.headerBodyDecoder = new Decoder();
            this.headerBodyContext = DecodeContext.factory.apply(heapPool, threadLocalBuffer);

            this.logDataCallback = new RaftLogDataCallback(logData -> {
                if (logData != null) {
                    if (result.logs == null) {
                        result.logs = new ArrayList<>();
                    }
                    RaftCodecFactory codecFactory = decoderFactory.apply(result.groupId);
                    if (codecFactory == null) {
                        log.error("can't find raft group codecFactory: {}", result.groupId);
                        throw new RaftException("can't find raft group codecFactory: " + result.groupId);
                    }
                    Object bizHeader = decode(true, codecFactory, logData.bizHeader, logData);
                    Object bizBody = decode(false, codecFactory, logData.bizBody, logData);
                    RaftTask task = new RaftTask(logData.type, logData.bizType, logData, bizHeader, bizBody,
                            null, logData.type == LogHeader.TYPE_LOG_READ, null);

                    // TODO optimise RaftTask.init()
                    task.term = logData.term;
                    task.prevLogTerm = logData.prevLogTerm;
                    task.index = logData.index;
                    task.timestamp = logData.timestamp;

                    result.logs.add(task);
                }
            });
        }

        @Override
        protected void begin(int len) {
            result = new AppendReq();
        }

        @Override
        protected void end(boolean success) {
            if (!success) {
                result.clean();
            }
            result = null;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case IDX_GROUP_ID:
                    result.groupId = (int) value;
                    break;
                case IDX_TERM:
                    result.term = (int) value;
                    break;
                case IDX_LEADER_ID:
                    result.leaderId = (int) value;
                    break;
                case IDX_PREV_LOG_TERM:
                    result.prevLogTerm = (int) value;
                    break;
                case IDX_LOGS_SIZE:
                    result.logs = createArrayList((int) value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case IDX_PREV_LOG_INDEX:
                    result.prevLogIndex = value;
                    break;
                case IDX_LEADER_COMMIT:
                    result.leaderCommit = value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int len, int currentPos) {
            if (index == IDX_ENTRIES) {
                parseNested(buf, len, currentPos, logDataCallback);
                if (context.createOrGetNestedDecoder().shouldSkip()) {
                    return false;
                }
            }
            return true;
        }

        private Object decode(boolean header, RaftCodecFactory codecFactory, RefBuffer rb, RaftLogData logData) {
            if (rb == null) {
                return null;
            }
            if (logData.type != LogHeader.TYPE_NORMAL) {
                ByteBuffer buf = rb.getBuffer();
                byte[] b = new byte[buf.remaining()];
                int p = buf.position();
                buf.get(b);
                buf.position(p);
                return b;
            }
            DecoderCallback<? extends Object> c = header ?
                    codecFactory.createHeaderCallback(logData.bizType, headerBodyContext) :
                    codecFactory.createBodyCallback(logData.bizType, headerBodyContext);
            if (c == null) {
                return null;
            }
            ByteBuffer buf = rb.getBuffer();
            int oldPos = buf.position();
            try {
                headerBodyDecoder.prepareNext(headerBodyContext, c);
                return headerBodyDecoder.decode(buf, buf.remaining(), 0);
            } finally {
                buf.position(oldPos);
                headerBodyContext.reset(headerBodyDecoder);
            }
        }

        @Override
        public AppendReq getResult() {
            return result;
        }
    }
}


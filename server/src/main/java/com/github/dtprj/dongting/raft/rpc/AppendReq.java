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

import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftRpcData;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.function.Function;

/**
 * @author huangli
 */
//message AppendEntriesReq {
//  uint32 group_id = 1;
//  uint32 term = 2;
//  uint32 leader_id = 3;
//  fixed64 prev_log_index = 4;
//  uint32 prev_log_term = 5;
//  fixed64 leader_commit = 6;
//  repeated LogItem entries = 7;
//}
public class AppendReq extends RaftRpcData {
    private static final DtLog log = DtLogs.getLogger(AppendReq.class);

    // private int groupId;
    // private int term;
    public int leaderId;
    public long prevLogIndex;
    public int prevLogTerm;
    public long leaderCommit;
    public final LinkedList<LogItem> logs = new LinkedList<>();

    // re-used
    public static class Callback extends PbCallback<AppendReq> {

        private final Function<Integer, RaftCodecFactory> decoderFactory;
        private AppendReq result;

        private final LogItemCallback logItemCallback = new LogItemCallback();

        public Callback(Function<Integer, RaftCodecFactory> decoderFactory) {
            this.decoderFactory = decoderFactory;
        }

        @Override
        protected void begin(int len) {
            result = new AppendReq();
        }

        @Override
        protected boolean end(boolean success) {
            if (!success) {
                RaftUtil.release(result.logs);
            }
            result = null;
            logItemCallback.codecFactory = null;
            return success;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    result.groupId = (int) value;
                    break;
                case 2:
                    result.term = (int) value;
                    break;
                case 3:
                    result.leaderId = (int) value;
                    break;
                case 5:
                    result.prevLogTerm = (int) value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case 4:
                    result.prevLogIndex = value;
                    break;
                case 6:
                    result.leaderCommit = value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int len, int currentPos) {
            boolean end = buf.remaining() >= len - currentPos;
            if (index == 7) {
                if (logItemCallback.codecFactory == null) {
                    logItemCallback.codecFactory = decoderFactory.apply(result.groupId);
                    if (logItemCallback.codecFactory == null) {
                        log.error("can't find raft group codecFactory: {}", result.groupId);
                        // cancel parse, so return null, but parent parser not canceled,
                        // we will get a ReadPacket with null body
                        return false;
                    }
                }
                LogItem i = (LogItem) parseNested(buf, len, currentPos, logItemCallback);
                if (end) {
                    result.logs.add(i);
                }
            }
            return true;
        }

        @Override
        public AppendReq getResult() {
            return result;
        }
    }

    //message LogItem {
    //  uint32 type = 1;
    //  uint32 bizType = 2;
    //  uint32 term = 3;
    //  fixed64 index = 4;
    //  uint32 prev_log_term = 5;
    //  fixed64 timestamp = 6;
    //  bytes header = 7;
    //  bytes body = 8;
    //}
    // re-used
    static class LogItemCallback extends PbCallback<Object> {
        private LogItem item;
        private RaftCodecFactory codecFactory;

        @Override
        protected void begin(int len) {
            item = new LogItem();
        }

        @Override
        protected boolean end(boolean success) {
            if (!success) {
                item.release();
            }
            item = null;
            return success;
        }

        @Override
        protected Object getResult() {
            return item;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    item.setType((int) value);
                    break;
                case 2:
                    item.setBizType((int) value);
                    break;
                case 3:
                    item.setTerm((int) value);
                    break;
                case 5:
                    item.setPrevLogTerm((int) value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case 4:
                    item.setIndex(value);
                    break;
                case 6:
                    item.setTimestamp(value);
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int len, int currentPos) {
            boolean begin = currentPos == 0;
            boolean end = buf.remaining() >= len - currentPos;
            DecoderCallback<? extends Encodable> currentDecoderCallback;
            if (index == 7) {
                if (begin) {
                    item.setActualHeaderSize(len);
                    if (item.getType() == LogItem.TYPE_NORMAL) {
                        currentDecoderCallback = codecFactory.createHeaderCallback(item.getBizType(), context.createOrGetNestedContext());
                    } else {
                        currentDecoderCallback = new ByteArray.Callback();
                    }
                } else {
                    currentDecoderCallback = null;
                }
                Encodable result = parseNested(buf, len, currentPos, currentDecoderCallback);
                if (end) {
                    item.setHeader(result);
                }
            } else if (index == 8) {
                if (begin) {
                    item.setActualBodySize(len);
                    if (item.getType() == LogItem.TYPE_NORMAL || item.getType() == LogItem.TYPE_LOG_READ) {
                        currentDecoderCallback = codecFactory.createBodyCallback(item.getBizType(), context.createOrGetNestedContext());
                    } else {
                        currentDecoderCallback = new ByteArray.Callback();
                    }
                } else {
                    currentDecoderCallback = null;
                }
                Encodable result = parseNested(buf, len, currentPos, currentDecoderCallback);
                if (end) {
                    item.setBody(result);
                }
            }
            return true;
        }
    }
}

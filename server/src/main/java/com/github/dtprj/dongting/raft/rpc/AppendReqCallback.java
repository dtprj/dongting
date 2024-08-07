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

import com.github.dtprj.dongting.codec.ByteArrayEncoder;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
public class AppendReqCallback extends PbCallback<AppendReqCallback> {

    private final DecodeContext context;
    private final Function<Integer, RaftCodecFactory> decoderFactory;
    private RaftCodecFactory raftCodecFactory;

    private int groupId;
    private int term;
    private int leaderId;
    private long prevLogIndex;
    private int prevLogTerm;
    private final ArrayList<LogItem> logs = new ArrayList<>();
    private long leaderCommit;


    public AppendReqCallback(DecodeContext context, Function<Integer, RaftCodecFactory> decoderFactory) {
        this.context = context;
        this.decoderFactory = decoderFactory;
    }

    @Override
    protected void end(boolean success) {
        if (!success) {
            RaftUtil.release(logs);
        }
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case 1:
                groupId = (int) value;
                break;
            case 2:
                term = (int) value;
                break;
            case 3:
                leaderId = (int) value;
                break;
            case 5:
                prevLogTerm = (int) value;
                break;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        switch (index) {
            case 4:
                prevLogIndex = value;
                break;
            case 6:
                leaderCommit = value;
                break;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int len, int currentPos) {
        boolean end = buf.remaining() >= len - currentPos;
        if (index == 7) {
            LogItemCallback callback = null;
            if (currentPos == 0) {
                if (raftCodecFactory == null) {
                    // group id should encode before entries
                    raftCodecFactory = decoderFactory.apply(groupId);
                    if (raftCodecFactory == null) {
                        throw new PbException("can't find raft group: " + groupId);
                    }
                }
                // since AppendReqCallback not use context (to save status), we can use it in sub parser
                callback = new LogItemCallback(context, raftCodecFactory);
            }
            callback = parseNested(index, buf, len, currentPos, callback);
            if (end) {
                LogItem i = callback.item;
                logs.add(i);
            }
        }
        return true;
    }

    @Override
    public AppendReqCallback getResult() {
        return this;
    }


    public int getGroupId() {
        return groupId;
    }

    public int getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public ArrayList<LogItem> getLogs() {
        return logs;
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
    static class LogItemCallback extends PbCallback<Object> {
        private final LogItem item = new LogItem();
        private final DecodeContext context;
        private final RaftCodecFactory codecFactory;
        private Decoder<? extends Encodable> currentDecoder;

        public LogItemCallback(DecodeContext context, RaftCodecFactory codecFactory) {
            this.context = context;
            this.codecFactory = codecFactory;
        }

        @Override
        protected void end(boolean success) {
            if (!success) {
                item.release();
            }
            resetDecoder();
            context.reset();
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
            if (index == 7) {
                if (begin) {
                    item.setActualHeaderSize(len);
                }
                Encodable result = null;
                if (item.getType() == LogItem.TYPE_NORMAL) {
                    currentDecoder = codecFactory.createHeaderDecoder(item.getBizType());
                    result = currentDecoder.decode(context, buf, len, currentPos);
                } else {
                    byte[] bytes = parseBytes(buf, len, currentPos);
                    context.setStatus(bytes);
                    if (bytes != null) {
                        result = new ByteArrayEncoder(bytes);
                    }
                }
                if (end) {
                    resetDecoder();
                    item.setHeader(result);
                }
            } else if (index == 8) {
                if (begin) {
                    item.setActualBodySize(len);
                }
                Encodable result = null;
                if (item.getType() == LogItem.TYPE_NORMAL) {
                    currentDecoder = codecFactory.createBodyDecoder(item.getBizType());
                    result = currentDecoder.decode(context, buf, len, currentPos);
                } else {
                    byte[] bytes = parseBytes(buf, len, currentPos);
                    context.setStatus(bytes);
                    if (bytes != null) {
                        result = new ByteArrayEncoder(bytes);
                    }
                }
                if (end) {
                    resetDecoder();
                    item.setBody(result);
                }
            }
            return true;
        }

        private void resetDecoder() {
            if (currentDecoder != null) {
                try {
                    currentDecoder.finish(context);
                } finally {
                    currentDecoder = null;
                }
            }
        }
    }
}

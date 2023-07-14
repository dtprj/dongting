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

import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbNoCopyDecoder;
import com.github.dtprj.dongting.codec.StrFiledDecoder;
import com.github.dtprj.dongting.dtkv.GetReq;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.ByteBufferWriteFrame;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespFrame;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.rpc.RaftGroupProcessor;
import com.github.dtprj.dongting.raft.server.NotLeaderException;
import com.github.dtprj.dongting.raft.server.RaftExecTimeoutException;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftNode;
import com.github.dtprj.dongting.raft.server.RaftServer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public class GetProcessor extends RaftGroupProcessor<GetReq> {
    private static final DtLog log = DtLogs.getLogger(GetProcessor.class);

    private static final PbNoCopyDecoder<GetReq> DECODER = new PbNoCopyDecoder<>(c -> new PbCallback<>() {
        private final GetReq result = new GetReq();

        @Override
        public boolean readFix32(int index, int value) {
            if (index == 1) {
                result.setKey(String.valueOf(value));
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == 2) {
                StrFiledDecoder.parseUTF8(c, buf, fieldLen, currentPos);
            }
            return true;
        }

        @Override
        public GetReq getResult() {
            return result;
        }
    });

    public GetProcessor(boolean runInCurrentThread, RaftServer server) {
        super(runInCurrentThread, server);
    }

    @Override
    public Decoder<GetReq> createDecoder() {
        return DECODER;
    }

    @Override
    protected int getGroupId(ReadFrame<GetReq> frame) {
        return frame.getBody().getGroupId();
    }

    /**
     * should run in io thread.
     */
    @Override
    public WriteFrame doProcess(ReadFrame<GetReq> frame, ChannelContext channelContext,
                                ReqContext reqContext, RaftGroup group) {
        group.getLogIndexForRead(reqContext.getTimeout()).whenComplete((logIndex, ex) -> {
            if (ex != null) {
                if (ex instanceof RaftExecTimeoutException) {
                    log.info("getLogIndexForRead timeout");
                    return;
                }
                EmptyBodyRespFrame error;
                if (ex instanceof NotLeaderException) {
                    error = new EmptyBodyRespFrame(CmdCodes.NOT_RAFT_LEADER);
                    RaftNode leader = ((NotLeaderException) ex).getCurrentLeader();
                    if (leader != null) {
                        String hpStr = leader.getHostPort().getHost() + ":" + leader.getHostPort().getPort();
                        error.setExtra(hpStr.getBytes(StandardCharsets.UTF_8));
                    }
                    log.warn("not leader, current leader is {}", leader);
                } else {
                    error = new EmptyBodyRespFrame(CmdCodes.BIZ_ERROR);
                    error.setMsg("getLogIndexForRead error: " + ex);
                    log.warn(error.getMsg());
                }
                channelContext.getRespWriter().writeRespInBizThreads(frame, error, reqContext.getTimeout());
            } else {
                DtKV dtKV = (DtKV) group.getStateMachine();
                byte[] bytes = dtKV.get(frame.getBody().getKey());
                ByteBufferWriteFrame wf = new ByteBufferWriteFrame(ByteBuffer.wrap(bytes));
                wf.setRespCode(CmdCodes.SUCCESS);
                channelContext.getRespWriter().writeRespInBizThreads(frame, wf, reqContext.getTimeout());
            }
        });
        return null;
    }

}

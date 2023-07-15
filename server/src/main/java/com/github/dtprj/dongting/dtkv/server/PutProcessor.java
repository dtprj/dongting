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

import com.github.dtprj.dongting.codec.ByteArrayDecoder;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbNoCopyDecoder;
import com.github.dtprj.dongting.codec.StrFiledDecoder;
import com.github.dtprj.dongting.dtkv.PutReq;
import com.github.dtprj.dongting.net.ChannelContext;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespFrame;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.rpc.RaftGroupProcessor;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.RaftServer;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class PutProcessor extends RaftGroupProcessor<PutReq> {

    private static final PbNoCopyDecoder<PutReq> decoder = new PbNoCopyDecoder<>(c -> new PbCallback<>() {

        private final PutReq result = new PutReq();

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == 1) {
                result.setGroupId((int) value);
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == 2) {
                result.setKey(StrFiledDecoder.parseUTF8(c, buf, fieldLen, currentPos));
            } else if (index == 3) {
                result.setValue(ByteArrayDecoder.decodeToArray(c, buf, fieldLen, currentPos));
            }
            return true;
        }

        @Override
        public PutReq getResult() {
            return result;
        }
    });

    public PutProcessor(RaftServer raftServer) {
        super(true, raftServer);
    }


    @Override
    public Decoder<PutReq> createDecoder() {
        return decoder;
    }

    @Override
    protected int getGroupId(ReadFrame<PutReq> frame) {
        return frame.getBody().getGroupId();
    }

    /**
     * run in io thread.
     */
    @Override
    protected WriteFrame doProcess(ReadFrame<PutReq> frame, ChannelContext channelContext,
                                   ReqContext reqContext, RaftGroup raftGroup) {
        PutReq req = frame.getBody();
        byte[] data = req.getValue();
        RaftInput ri = new RaftInput(DtKV.BIZ_TYPE_PUT, req.getKey(), data,
                reqContext.getTimeout(), data == null ? 0 : data.length);
        CompletableFuture<RaftOutput> f = raftGroup.submitLinearTask(ri);
        f.whenComplete((output, ex) -> {
            if (ex != null) {
                RaftUtil.processError(frame, channelContext, reqContext, ex);
            } else {
                EmptyBodyRespFrame resp = new EmptyBodyRespFrame(CmdCodes.SUCCESS);
                channelContext.getRespWriter().writeRespInBizThreads(frame, resp, reqContext.getTimeout());
            }
        });
        return null;
    }
}

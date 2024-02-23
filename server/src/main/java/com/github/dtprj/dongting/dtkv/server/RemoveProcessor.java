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
import com.github.dtprj.dongting.dtkv.RemoveReq;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.PbIntWriteFrame;
import com.github.dtprj.dongting.net.ReadFrame;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.WriteFrame;
import com.github.dtprj.dongting.raft.server.AbstractRaftBizProcessor;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftOutput;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class RemoveProcessor extends AbstractRaftBizProcessor<RemoveReq> {

    private static final PbNoCopyDecoder<RemoveReq> DECODER = new PbNoCopyDecoder<>(c -> new PbCallback<>() {
        private final RemoveReq result = new RemoveReq();
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
                result.setKey(StrFiledDecoder.INSTANCE.decode(c, buf, fieldLen, currentPos));
            }
            return true;
        }

        @Override
        public RemoveReq getResult() {
            return result;
        }
    });

    public RemoveProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    @Override
    public Decoder<RemoveReq> createDecoder(int cmd) {
        return DECODER;
    }

    @Override
    protected int getGroupId(ReadFrame<RemoveReq> frame) {
        return frame.getBody().getGroupId();
    }

    @Override
    protected WriteFrame doProcess(ReqInfo<RemoveReq> reqInfo) {
        RemoveReq req = reqInfo.getReqFrame().getBody();
        ReqContext reqContext = reqInfo.getReqContext();
        RaftInput ri = new RaftInput(DtKV.BIZ_TYPE_REMOVE, req.getKey(), null,
                reqContext.getTimeout(), 0);
        CompletableFuture<RaftOutput> f = reqInfo.getRaftGroup().submitLinearTask(ri);
        f.whenComplete((output, ex) -> {
            if (ex != null) {
                processError(reqInfo, ex);
            } else {
                Boolean result = (Boolean) output.getResult();
                PbIntWriteFrame resp = new PbIntWriteFrame(result ? 1 : 0);
                resp.setRespCode(CmdCodes.SUCCESS);
                writeResp(reqInfo, resp);
            }
        });
        return null;
    }
}

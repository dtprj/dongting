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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbNoCopyDecoderCallback;
import com.github.dtprj.dongting.dtkv.GetReq;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RefBufWritePacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.server.AbstractRaftBizProcessor;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class GetProcessor extends AbstractRaftBizProcessor<GetReq> {

    private static final PbNoCopyDecoderCallback<GetReq> DECODER = new PbNoCopyDecoderCallback<>(c -> new PbCallback<>() {
        private final GetReq result = new GetReq();

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
                result.setKey(parseUTF8(buf, fieldLen, currentPos));
            }
            return true;
        }

        @Override
        public GetReq getResult() {
            return result;
        }
    });

    public GetProcessor(RaftServer server) {
        super(server);
    }

    @Override
    public DecoderCallback<GetReq> createDecoder(int cmd) {
        return DECODER;
    }

    @Override
    protected int getGroupId(ReadPacket<GetReq> frame) {
        return frame.getBody().getGroupId();
    }

    /**
     * run in io thread.
     */
    @Override
    protected WritePacket doProcess(ReqInfo<GetReq> reqInfo) {
        ReadPacket<GetReq> frame = reqInfo.getReqFrame();
        ReqContext reqContext = reqInfo.getReqContext();
        RaftGroup group = reqInfo.getRaftGroup();
        group.getLeaseReadIndex(reqContext.getTimeout()).whenComplete((logIndex, ex) -> {
            if (ex != null) {
                processError(reqInfo, ex);
            } else {
                DtKV dtKV = (DtKV) group.getStateMachine();
                RefBuffer rb = dtKV.get(logIndex, frame.getBody().getKey());
                RefBufWritePacket wf = new RefBufWritePacket(rb);
                wf.setRespCode(CmdCodes.SUCCESS);
                writeResp(reqInfo, wf);
            }
        });
        return null;
    }

}

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
import com.github.dtprj.dongting.codec.StrEncoder;
import com.github.dtprj.dongting.dtkv.PutReq;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.server.AbstractRaftBizProcessor;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class PutProcessor extends AbstractRaftBizProcessor<PutReq> {

    private static final PbNoCopyDecoder<PutReq> DECODER = new PbNoCopyDecoder<>(c -> new PbCallback<>() {

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
                result.setKey(parseUTF8(buf, fieldLen, currentPos));
            } else if (index == 3) {
                result.setValue(parseRefBuffer(buf, fieldLen, currentPos));
            }
            return true;
        }

        @Override
        public PutReq getResult() {
            return result;
        }
    });

    public PutProcessor(RaftServer raftServer) {
        super(raftServer);
    }


    @Override
    public Decoder<PutReq> createDecoder(int cmd) {
        return DECODER;
    }

    @Override
    protected int getGroupId(ReadPacket<PutReq> frame) {
        return frame.getBody().getGroupId();
    }

    /**
     * run in io thread.
     */
    @Override
    protected WritePacket doProcess(ReqInfo<PutReq> reqInfo) {
        PutReq req = reqInfo.getReqFrame().getBody();
        ReqContext reqContext = reqInfo.getReqContext();
        RaftInput ri = new RaftInput(DtKV.BIZ_TYPE_PUT, new StrEncoder(req.getKey()), req.getValue(),
                reqContext.getTimeout(), false);
        RaftCallback c = new RaftCallback() {
            @Override
            public void success(long raftIndex, Object result) {
                EmptyBodyRespPacket resp = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
                writeResp(reqInfo, resp);
            }

            @Override
            public void fail(Throwable ex) {
                processError(reqInfo, ex);
            }
        };
        reqInfo.getRaftGroup().submitLinearTask(ri, c);
        return null;
    }
}

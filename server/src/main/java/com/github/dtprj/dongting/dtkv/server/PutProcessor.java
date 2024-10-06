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

import com.github.dtprj.dongting.codec.ByteArrayEncoder;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import com.github.dtprj.dongting.raft.server.AbstractRaftBizProcessor;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

/**
 * @author huangli
 */
public class PutProcessor extends AbstractRaftBizProcessor<KvReq> {


    public PutProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    @Override
    public DecoderCallback<KvReq> createDecoderCallback(int cmd, DecodeContext context) {
        DecodeContextEx e = (DecodeContextEx) context;
        return context.toDecoderCallback(e.kvReqCallback());
    }

    @Override
    protected int getGroupId(ReadPacket<KvReq> frame) {
        return frame.getBody().getGroupId();
    }

    /**
     * run in io thread.
     */
    @Override
    protected WritePacket doProcess(ReqInfo<KvReq> reqInfo) {
        KvReq req = reqInfo.getReqFrame().getBody();
        ReqContext reqContext = reqInfo.getReqContext();
        RaftInput ri = new RaftInput(DtKV.BIZ_TYPE_PUT, new ByteArrayEncoder(req.getKey()), req.getValue(),
                reqContext.getTimeout(), false);
        RaftCallback c = new RaftCallback() {
            @Override
            public void success(long raftIndex, Object result) {
                KvResult r = (KvResult) result;
                EmptyBodyRespPacket resp = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
                resp.setBizCode(r.getBizCode());
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

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

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.dtkv.KvResp;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

/**
 * @author huangli
 */
public class KvProcessor extends RaftProcessor<KvReq> {
    public KvProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    @Override
    public DecoderCallback<KvReq> createDecoderCallback(int cmd, DecodeContext context) {
        DecodeContextEx e = (DecodeContextEx) context;
        return context.toDecoderCallback(e.kvReqCallback());
    }

    @Override
    protected int getGroupId(ReadPacket<KvReq> frame) {
        return frame.getBody().groupId;
    }

    /**
     * run in io thread.
     */
    @Override
    protected WritePacket doProcess(ReqInfo<KvReq> reqInfo) {
        ReadPacket<KvReq> frame = reqInfo.reqFrame;
        KvReq req = frame.getBody();
        if (req == null) {
            EmptyBodyRespPacket errorResp = new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR);
            errorResp.setMsg("request body is null");
            return errorResp;
        }
        switch (frame.getCommand()) {
            case Commands.DTKV_GET:
                leaseRead(reqInfo, (dtKV, kvReq) -> {
                    KvResult r = dtKV.get(new ByteArray(kvReq.key));
                    KvResp resp = new KvResp(Collections.singletonList(r));
                    EncodableBodyWritePacket p = new EncodableBodyWritePacket(resp);
                    p.setRespCode(CmdCodes.SUCCESS);
                    p.setBizCode(r.getBizCode());
                    return p;
                });
                break;
            case Commands.DTKV_PUT:
                submitWriteTask(reqInfo, DtKV.BIZ_TYPE_PUT, new ByteArray(req.key), new ByteArray(req.value));
                break;
            case Commands.DTKV_REMOVE:
                submitWriteTask(reqInfo, DtKV.BIZ_TYPE_REMOVE, new ByteArray(req.key), null);
                break;
            case Commands.DTKV_MKDIR:
                submitWriteTask(reqInfo, DtKV.BIZ_TYPE_MKDIR, new ByteArray(req.key), null);
                break;
            case Commands.DTKV_LIST:
                leaseRead(reqInfo, (dtKV, kvReq) -> {
                    Pair<Integer, List<KvResult>> r = dtKV.list(req.key == null ? null : new ByteArray(req.key));
                    List<KvResult> results = r.getRight();
                    if (results == null) {
                        EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
                        p.setBizCode(r.getLeft());
                        return p;
                    } else {
                        KvResp resp = new KvResp(results);
                        EncodableBodyWritePacket p = new EncodableBodyWritePacket(resp);
                        p.setRespCode(CmdCodes.SUCCESS);
                        p.setBizCode(KvCodes.CODE_SUCCESS);
                        return p;
                    }
                });
                break;
            default:
                throw new RaftException("unknown command: " + frame.getCommand());
        }
        return null;
    }

    private void leaseRead(ReqInfo<KvReq> reqInfo, BiFunction<DtKV, KvReq, WritePacket> f) {
        reqInfo.raftGroup.leaseRead(reqInfo.reqContext.getTimeout(), (result, ex) -> {
            if (ex == null) {
                try {
                    DtKV dtKV = (DtKV) reqInfo.raftGroup.getStateMachine();
                    WritePacket p = f.apply(dtKV, reqInfo.reqFrame.getBody());
                    reqInfo.reqContext.writeRespInBizThreads(p);
                } catch (Exception e) {
                    writeErrorResp(reqInfo, e);
                }
            } else {
                writeErrorResp(reqInfo, ex);
            }
        });
    }

    private void submitWriteTask(ReqInfo<KvReq> reqInfo, int bizType, Encodable header, Encodable body) {
        RaftInput ri = new RaftInput(bizType, header, body, reqInfo.reqContext.getTimeout(), false);
        reqInfo.raftGroup.submitLinearTask(ri, new RC(reqInfo));
    }

    private class RC implements RaftCallback {

        private final ReqInfo<KvReq> reqInfo;

        private RC(ReqInfo<KvReq> reqInfo) {
            this.reqInfo = reqInfo;
        }

        @Override
        public void success(long raftIndex, Object result) {
            KvResult r = (KvResult) result;
            EmptyBodyRespPacket resp = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            resp.setBizCode(r.getBizCode());
            reqInfo.reqContext.writeRespInBizThreads(resp);
        }

        @Override
        public void fail(Throwable ex) {
            writeErrorResp(reqInfo, ex);
        }
    }

}

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
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.common.Pair;
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
import com.github.dtprj.dongting.raft.server.RaftBizProcessor;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.util.Collections;
import java.util.List;

/**
 * @author huangli
 */
public class KvProcessor extends RaftBizProcessor<KvReq> {
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
        return frame.getBody().getGroupId();
    }

    /**
     * run in io thread.
     */
    @Override
    protected WritePacket doProcess(ReqInfo<KvReq> reqInfo) {
        ReadPacket<KvReq> frame = reqInfo.getReqFrame();
        KvReq req = frame.getBody();
        if (req == null) {
            EmptyBodyRespPacket errorResp = new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR);
            errorResp.setMsg("request body is null");
            return errorResp;
        }
        switch (frame.getCommand()) {
            case Commands.DTKV_GET:
                doGet(reqInfo, req);
                break;
            case Commands.DTKV_PUT:
                submitWriteTask(reqInfo, DtKV.BIZ_TYPE_PUT, new ByteArray(req.getKey()), req.getValue());
                break;
            case Commands.DTKV_REMOVE:
                submitWriteTask(reqInfo, DtKV.BIZ_TYPE_REMOVE, new ByteArray(req.getKey()), null);
                break;
            case Commands.DTKV_MKDIR:
                submitWriteTask(reqInfo, DtKV.BIZ_TYPE_MKDIR, new ByteArray(req.getKey()), null);
                break;
            case Commands.DTKV_LIST:
                doList(reqInfo, req);
                break;
            default:
                throw new RaftException("unknown command: " + frame.getCommand());
        }
        return null;
    }

    private void doGet(ReqInfo<KvReq> reqInfo, KvReq req) {
        reqInfo.getRaftGroup().leaseRead(reqInfo.getReqContext().getTimeout(), new FutureCallback<>() {
            @Override
            public void success(Long result) {
                try {
                    DtKV dtKV = (DtKV) reqInfo.getRaftGroup().getStateMachine();
                    KvResult r = dtKV.get(new ByteArray(req.getKey()));
                    KvResp resp = new KvResp(Collections.singletonList(r));
                    EncodableBodyWritePacket wf = new EncodableBodyWritePacket(resp);
                    wf.setRespCode(CmdCodes.SUCCESS);
                    wf.setBizCode(r.getBizCode());
                    writeResp(reqInfo, wf);
                } catch (Exception e) {
                    writeErrorResp(reqInfo, e);
                }
            }

            @Override
            public void fail(Throwable ex) {
                writeErrorResp(reqInfo, ex);
            }
        });
    }

    private void doList(ReqInfo<KvReq> reqInfo, KvReq req) {

        reqInfo.getRaftGroup().leaseRead(reqInfo.getReqContext().getTimeout(), new FutureCallback<>() {
            @Override
            public void success(Long result) {
                try {
                    DtKV dtKV = (DtKV) reqInfo.getRaftGroup().getStateMachine();
                    Pair<Integer, List<KvResult>> p = dtKV.list(req.getKey() == null ? null : new ByteArray(req.getKey()));
                    KvResp resp = new KvResp(p.getRight());
                    EncodableBodyWritePacket wf = new EncodableBodyWritePacket(resp);
                    wf.setRespCode(CmdCodes.SUCCESS);
                    wf.setBizCode(p.getLeft());
                    writeResp(reqInfo, wf);
                } catch (Exception e) {
                    writeErrorResp(reqInfo, e);
                }
            }

            @Override
            public void fail(Throwable ex) {
                writeErrorResp(reqInfo, ex);
            }
        });
    }

    private void submitWriteTask(ReqInfo<KvReq> reqInfo, int bizType, Encodable header, Encodable body) {
        RaftInput ri = new RaftInput(bizType, header, body, reqInfo.getReqContext().getTimeout(), false);
        reqInfo.getRaftGroup().submitLinearTask(ri, new RC(reqInfo));
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
            writeResp(reqInfo, resp);
        }

        @Override
        public void fail(Throwable ex) {
            writeErrorResp(reqInfo, ex);
        }
    }

}

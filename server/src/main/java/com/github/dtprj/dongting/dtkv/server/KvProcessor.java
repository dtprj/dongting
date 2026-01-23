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
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.dtkv.KvResp;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RetryableWritePacket;
import com.github.dtprj.dongting.net.WorkerThread;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author huangli
 */
final class KvProcessor extends RaftProcessor<KvReq> {
    public KvProcessor(RaftServer raftServer) {
        super(raftServer, true, false);
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
        DtKV dtKV = KvServerUtil.getStateMachine(reqInfo);
        if (dtKV == null) {
            return null;
        }
        ReadPacket<KvReq> frame = reqInfo.reqFrame;
        KvReq req = frame.getBody();

        // use uuid initialized in handshake
        req.ownerUuid = reqInfo.reqContext.getDtChannel().getRemoteUuid();

        try {
            switch (frame.command) {
                case Commands.DTKV_GET:
                    leaseRead(reqInfo, (raftIndex, kvReq) -> {
                        KvResult r = dtKV.get(kvReq.key == null ? null : new ByteArray(kvReq.key));
                        KvResp resp = new KvResp(raftIndex, Collections.singletonList(r));
                        EncodableBodyWritePacket p = new EncodableBodyWritePacket(resp);
                        p.respCode = CmdCodes.SUCCESS;
                        p.bizCode = r.getBizCode();
                        return p;
                    });
                    break;
                case Commands.DTKV_PUT:
                    submitWriteTask(reqInfo, DtKV.BIZ_TYPE_PUT, req);
                    break;
                case Commands.DTKV_REMOVE:
                    submitWriteTask(reqInfo, DtKV.BIZ_TYPE_REMOVE, req);
                    break;
                case Commands.DTKV_MKDIR:
                    submitWriteTask(reqInfo, DtKV.BIZ_TYPE_MKDIR, req);
                    break;
                case Commands.DTKV_LIST:
                    leaseRead(reqInfo, (raftIndex, kvReq) ->
                            convertMultiResult(raftIndex, dtKV.list(kvReq.key == null ? null : new ByteArray(kvReq.key))));
                    break;
                case Commands.DTKV_BATCH_GET:
                    leaseRead(reqInfo, (raftIndex, kvReq) ->
                            convertMultiResult(raftIndex, dtKV.batchGet(kvReq.keys)));
                    break;
                case Commands.DTKV_BATCH_PUT:
                    submitWriteTask(reqInfo, DtKV.BIZ_TYPE_BATCH_PUT, req);
                    break;
                case Commands.DTKV_BATCH_REMOVE:
                    submitWriteTask(reqInfo, DtKV.BIZ_TYPE_BATCH_REMOVE, req);
                    break;
                case Commands.DTKV_CAS:
                    submitWriteTask(reqInfo, DtKV.BIZ_TYPE_CAS, req);
                    break;
                case Commands.DTKV_PUT_TEMP_NODE:
                    checkTtlAndSubmit(reqInfo, DtKV.BIZ_TYPE_PUT_TEMP_NODE, req);
                    break;
                case Commands.DTKV_MAKE_TEMP_DIR:
                    checkTtlAndSubmit(reqInfo, DtKV.BIZ_MK_TEMP_DIR, req);
                    break;
                case Commands.DTKV_UPDATE_TTL:
                    checkTtlAndSubmit(reqInfo, DtKV.BIZ_TYPE_UPDATE_TTL, req);
                    break;
                case Commands.DTKV_TRY_LOCK:
                    checkTtlAndSubmit(reqInfo, DtKV.BIZ_TYPE_TRY_LOCK, req);
                    break;
                case Commands.DTKV_UNLOCK:
                    submitWriteTask(reqInfo, DtKV.BIZ_TYPE_UNLOCK, req);
                    break;
                case Commands.DTKV_UPDATE_LOCK_LEASE:
                    checkTtlAndSubmit(reqInfo, DtKV.BIZ_TYPE_UPDATE_LOCK_LEASE, req);
                    break;
                default:
                    throw new RaftException("unknown command: " + frame.command);
            }
        } catch (Exception e) {
            writeErrorResp(reqInfo, e);
        }
        return null;
    }

    private void checkTtlAndSubmit(ReqInfo<KvReq> reqInfo, int bizType, KvReq req) {
        String errorMsg = KvImpl.checkTtl(req.ttlMillis, req.value, bizType == DtKV.BIZ_TYPE_TRY_LOCK);
        if (errorMsg != null) {
            EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            p.bizCode = KvCodes.INVALID_TTL;
            p.msg = errorMsg;
            reqInfo.reqContext.writeRespInBizThreads(p);
        } else {
            submitWriteTask(reqInfo, bizType, req);
        }
    }

    private RetryableWritePacket convertMultiResult(long raftIndex, Pair<Integer, List<KvResult>> r) {
        List<KvResult> results = r.getRight();
        if (results == null) {
            EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            p.bizCode = r.getLeft();
            return p;
        } else {
            KvResp resp = new KvResp(raftIndex, results);
            EncodableBodyWritePacket p = new EncodableBodyWritePacket(resp);
            p.respCode = CmdCodes.SUCCESS;
            p.bizCode = KvCodes.SUCCESS;
            return p;
        }
    }

    /**
     * the callback may run in other thread (raft thread etc.).
     */
    private void leaseRead(ReqInfo<KvReq> reqInfo, LeaseCallback callback) {
        // run in io thread, so we should use ts of io worker
        Timestamp ts = ((WorkerThread) Thread.currentThread()).ts;
        reqInfo.raftGroup.leaseRead(ts, reqInfo.reqContext.getTimeout(), (lastApplied, ex) -> {
            if (ex == null) {
                try {
                    WritePacket p = callback.apply(lastApplied, reqInfo.reqFrame.getBody());
                    reqInfo.reqContext.writeRespInBizThreads(p);
                } catch (Exception e) {
                    writeErrorResp(reqInfo, e);
                }
            } else {
                writeErrorResp(reqInfo, ex);
            }
        });
    }

    @FunctionalInterface
    private interface LeaseCallback {
        WritePacket apply(long raftIndex, KvReq kvReq);
    }

    private void submitWriteTask(ReqInfo<KvReq> reqInfo, int bizType, Encodable body) {
        RC ri = new RC(bizType, body, reqInfo);
        reqInfo.raftGroup.submitLinearTask(ri, ri);
    }

    private class RC extends RaftInput implements RaftCallback {

        private ReqInfo<KvReq> reqInfo;

        private RC(int bizType, Encodable body, ReqInfo<KvReq> reqInfo) {
            super(bizType, null, body, reqInfo.reqContext.getTimeout(), false);
            this.reqInfo = reqInfo;
        }

        @Override
        public void success(long raftIndex, Object result) {
            try {
                WritePacket resp;
                switch (reqInfo.reqFrame.command) {
                    case Commands.DTKV_PUT:
                    case Commands.DTKV_REMOVE:
                    case Commands.DTKV_MKDIR:
                    case Commands.DTKV_CAS:
                    case Commands.DTKV_PUT_TEMP_NODE:
                    case Commands.DTKV_MAKE_TEMP_DIR:
                    case Commands.DTKV_UPDATE_TTL:
                    case Commands.DTKV_UPDATE_LOCK_LEASE:
                    case Commands.DTKV_TRY_LOCK: {
                        KvResult r = (KvResult) result;
                        resp = new EncodableBodyWritePacket(new KvResp(raftIndex, Collections.singletonList(r)));
                        resp.respCode = CmdCodes.SUCCESS;
                        resp.bizCode = r.getBizCode();
                        break;
                    }
                    case Commands.DTKV_BATCH_PUT:
                    case Commands.DTKV_BATCH_REMOVE: {
                        //noinspection unchecked
                        Pair<Integer, List<KvResult>> p = (Pair<Integer, List<KvResult>>) result;
                        List<KvResult> results = p.getLeft() == KvCodes.SUCCESS ?
                                new ArrayList<>(p.getRight()) : Collections.emptyList();
                        resp = new EncodableBodyWritePacket(new KvResp(raftIndex, results));
                        resp.respCode = CmdCodes.SUCCESS;
                        resp.bizCode = p.getLeft();
                        break;
                    }
                    case Commands.DTKV_UNLOCK: {
                        KvResult ri = (KvResult) result;

                        // send response first
                        resp = new EncodableBodyWritePacket(new KvResp(raftIndex, Collections.singletonList(ri)));
                        resp.respCode = CmdCodes.SUCCESS;
                        resp.bizCode = ri.getBizCode();
                        reqInfo.reqContext.writeRespInBizThreads(resp);

                        // notify the new lock owner if any
                        if(ri instanceof KvImpl.KvResultWithNewOwnerInfo) {
                            KvServerUtil.notifyNewLockOwner(reqInfo.raftGroup, (KvImpl.KvResultWithNewOwnerInfo) ri);
                        }

                        return; // the response is sent, so here use return
                    }
                    default:
                        resp = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
                        resp.msg = "unknown command: " + reqInfo.reqFrame.command;
                }
                reqInfo.reqContext.writeRespInBizThreads(resp);
            } finally {
                reqInfo = null;
            }
        }

        @Override
        public void fail(Throwable ex) {
            try {
                writeErrorResp(reqInfo, ex);
            } finally {
                reqInfo = null;
            }
        }
    }

}

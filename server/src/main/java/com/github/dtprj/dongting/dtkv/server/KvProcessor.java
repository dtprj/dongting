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
import com.github.dtprj.dongting.dtkv.*;
import com.github.dtprj.dongting.net.*;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author huangli
 */
final class KvProcessor extends RaftProcessor<KvReq> {
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
        if(errorMsg!=null){
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
        RaftInput ri = new RaftInput(bizType, null, body, reqInfo.reqContext.getTimeout(), false);
        reqInfo.raftGroup.submitLinearTask(ri, new RC(reqInfo));
    }

    private class RC implements RaftCallback {

        private final ReqInfo<KvReq> reqInfo;

        private RC(ReqInfo<KvReq> reqInfo) {
            this.reqInfo = reqInfo;
        }

        @Override
        public void success(long raftIndex, Object result) {
            WritePacket resp;
            switch (reqInfo.reqFrame.command) {
                case Commands.DTKV_PUT:
                case Commands.DTKV_REMOVE:
                case Commands.DTKV_MKDIR:
                case Commands.DTKV_CAS:
                case Commands.DTKV_PUT_TEMP_NODE:
                case Commands.DTKV_MAKE_TEMP_DIR:
                case Commands.DTKV_UPDATE_TTL:
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
                    KvResult r = (KvResult) result;
                    KvNode newOwner = r.getNode();
                    // remove nodes info
                    r = new KvResult(r.getBizCode());

                    // send response first
                    resp = new EncodableBodyWritePacket(new KvResp(raftIndex, Collections.singletonList(r)));
                    resp.respCode = CmdCodes.SUCCESS;
                    resp.bizCode = r.getBizCode();
                    reqInfo.reqContext.writeRespInBizThreads(resp);

                    // notify the new lock owner if any
                    if (newOwner != null) {
                        notifyNewLockOwner(newOwner, reqInfo);
                    }

                    return; // the response is sent, so here use return
                }
                default:
                    resp = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
                    resp.msg = "unknown command: " + reqInfo.reqFrame.command;
            }
            reqInfo.reqContext.writeRespInBizThreads(resp);
        }

        @Override
        public void fail(Throwable ex) {
            writeErrorResp(reqInfo, ex);
        }
    }

    private void notifyNewLockOwner(KvNode newOwner, ReqInfo<KvReq> reqInfo) {
        // Convert KvNode to KvNodeEx to access ttlInfo
        KvNodeEx newOwnerEx = (KvNodeEx) newOwner;

        // ttlInfo must not be null at this point
        if (newOwnerEx.ttlInfo != null) {
            // Get NioServer from RaftStatusImpl
            RaftStatusImpl raftStatus = ((RaftGroupImpl) reqInfo.raftGroup).groupComponents.raftStatus;

            // Notify the new lock owner
            DtKV.notifyNewLockOwner(raftStatus.serviceNioServer, newOwnerEx.ttlInfo.key,
                    newOwner.data, reqInfo.raftGroup.getGroupId());
        }
    }

}

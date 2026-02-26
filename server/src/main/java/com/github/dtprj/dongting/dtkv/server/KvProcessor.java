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
import com.github.dtprj.dongting.common.PerfCallback;
import com.github.dtprj.dongting.common.PerfConsts;
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
import com.github.dtprj.dongting.net.WorkerThread;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
final class KvProcessor extends RaftProcessor<KvReq> {
    private final PerfCallback perfCallback;

    public KvProcessor(RaftServer raftServer, PerfCallback perfCallback) {
        super(raftServer, true, false);
        Objects.requireNonNull(perfCallback);
        this.perfCallback = perfCallback;
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

        // use uuid initialized in handshake
        req.ownerUuid = reqInfo.reqContext.getDtChannel().getRemoteUuid();

        try {
            switch (frame.command) {
                case Commands.DTKV_GET:
                case Commands.DTKV_BATCH_GET:
                case Commands.DTKV_LIST:
                    leaseRead(reqInfo, req);
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

    private WritePacket convertMultiResult(long raftIndex, Pair<Integer, List<KvResult>> r) {
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

    // run in io thread
    private void leaseRead(ReqInfo<KvReq> reqInfo, KvReq req) {
        DtKV dtKV = KvServerUtil.getStateMachine(reqInfo);
        if (dtKV == null) {
            // response write in getStateMachine method, return null to indicate not write response
            return;
        }

        long startTime = perfCallback.takeTime(PerfConsts.DTKV_LEASE_READ);

        if (dtKV.kvConfig.readInDtKvExecutor) {
            submitToDtKvExecutor(reqInfo, req, dtKV, startTime);
        } else {
            // run in io thread, so we should use ts of io worker
            Timestamp ts = ((WorkerThread) Thread.currentThread()).ts;
            leaseRead0(dtKV, reqInfo, req, startTime, ts);
        }
    }

    private void submitToDtKvExecutor(ReqInfo<KvReq> reqInfo, KvReq req, DtKV dtKV, long startTime) {
        // use dtKV.ts, the ts bound to dtkv executor
        boolean b = dtKV.dtkvExecutor.submitTaskInAnyThread(() -> leaseRead0(dtKV, reqInfo, req, startTime, dtKV.ts));
        if (!b) {
            perfCallback.fireTime(PerfConsts.DTKV_LEASE_READ, startTime);
            writeErrorResp(reqInfo, new RaftException("dtkv executor is stopping"));
        }
    }

    private void leaseRead0(DtKV dtKV, ReqInfo<KvReq> reqInfo, KvReq req, long startTime, Timestamp ts) {
        WritePacket p;
        try {
            boolean b = reqInfo.raftGroup.isLeaseReadValid(ts, reqInfo.reqContext.getTimeout());
            if (b) {
                p = doLeaseRead(dtKV, reqInfo, req);
                perfCallback.fireTime(PerfConsts.DTKV_LEASE_READ, startTime);
            } else {
                CompletableFuture<Void> f = reqInfo.raftGroup.addGroupReadyListener(reqInfo.reqContext.getTimeout());
                // the future completed in raft thread
                f.whenComplete((v, ex) -> groupReadyCallback(dtKV, reqInfo, req, startTime, ex));
                return;
            }
        } catch (Exception e) {
            perfCallback.fireTime(PerfConsts.DTKV_LEASE_READ, startTime);
            writeErrorResp(reqInfo, e);
            return;
        }
        reqInfo.reqContext.writeRespInBizThreads(p);
    }

    // run in raft thread
    private void groupReadyCallback(DtKV dtKV, ReqInfo<KvReq> reqInfo, KvReq req, long startTime, Throwable ex) {
        if (ex == null) {
            if (dtKV.kvConfig.readInDtKvExecutor) {
                submitToDtKvExecutor(reqInfo, req, dtKV, startTime);
            } else {
                Timestamp timestampInGroup = ((RaftGroupImpl) reqInfo.raftGroup).groupComponents.raftStatus.ts;
                leaseRead0(dtKV, reqInfo, req, startTime, timestampInGroup);
            }
        } else {
            perfCallback.fireTime(PerfConsts.DTKV_LEASE_READ, startTime);
            writeErrorResp(reqInfo, ex);
        }
    }

    private WritePacket doLeaseRead(DtKV dtKV, ReqInfo<KvReq> reqInfo, KvReq req) {
        long raftIndex = 0; // read operations do not return raftIndex to client
        switch (reqInfo.reqFrame.command) {
            case Commands.DTKV_GET:
                KvResult r = dtKV.get(req.key == null ? null : new ByteArray(req.key));
                KvResp resp = new KvResp(raftIndex, Collections.singletonList(r));
                EncodableBodyWritePacket p = new EncodableBodyWritePacket(resp);
                p.respCode = CmdCodes.SUCCESS;
                p.bizCode = r.getBizCode();
                return p;
            case Commands.DTKV_BATCH_GET:
                return convertMultiResult(raftIndex, dtKV.batchGet(req.keys));
            case Commands.DTKV_LIST:
                return convertMultiResult(raftIndex, dtKV.list(req.key == null ? null : new ByteArray(req.key)));
            default:
                throw new RaftException("unknown command: " + reqInfo.reqFrame.command);
        }
    }

    private void submitWriteTask(ReqInfo<KvReq> reqInfo, int bizType, Encodable body) {
        RC ri = new RC(bizType, body, reqInfo);
        reqInfo.raftGroup.submitLinearTask(ri, ri);
    }

    private class RC extends RaftInput implements RaftCallback {

        private ReqInfo<KvReq> reqInfo;
        private final long startTime;

        private RC(int bizType, Encodable body, ReqInfo<KvReq> reqInfo) {
            super(bizType, null, body, reqInfo.reqContext.getTimeout(), false);
            this.reqInfo = reqInfo;
            this.startTime = perfCallback.takeTime(PerfConsts.DTKV_LINEARIZABLE_OP);
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
                        if (ri instanceof KvImpl.KvResultWithNewOwnerInfo) {
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
                perfCallback.fireTime(PerfConsts.DTKV_LINEARIZABLE_OP, startTime);
                reqInfo = null;
            }
        }

        @Override
        public void fail(Throwable ex) {
            try {
                writeErrorResp(reqInfo, ex);
            } finally {
                perfCallback.fireTime(PerfConsts.DTKV_LINEARIZABLE_OP, startTime);
                reqInfo = null;
            }
        }
    }

}

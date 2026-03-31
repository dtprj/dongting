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
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
final class KvReadProcessor extends RaftProcessor<KvReq> {
    private final PerfCallback perfCallback;

    public KvReadProcessor(RaftServer raftServer, PerfCallback perfCallback) {
        super(raftServer, true, false);
        Objects.requireNonNull(perfCallback);
        this.perfCallback = perfCallback;
    }

    @Override
    public DecoderCallback<KvReq> createDecoderCallback(int cmd, DecodeContext context) {
        DecodeContextEx e = (DecodeContextEx) context;
        return context.toDecoderCallback(e.kvReqCallback());
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
                default:
                    throw new RaftException("unknown command: " + frame.command);
            }
        } catch (Exception e) {
            writeErrorResp(reqInfo, e);
        }
        return null;
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

    private void submitToDtKvExecutor(ReqInfo<KvReq> reqInfo, KvReq req, DtKV dtKV, long startTime) {
        // use dtKV.ts, the ts bound to dtkv executor
        boolean b = dtKV.dtkvExecutor.submitTaskInAnyThread(() -> leaseRead0(dtKV, reqInfo, req, startTime, dtKV.ts));
        if (!b) {
            perfCallback.fireTime(PerfConsts.DTKV_LEASE_READ, startTime);
            writeErrorResp(reqInfo, new RaftException("dtkv executor is stopping"));
        }
    }
}

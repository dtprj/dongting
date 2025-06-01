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
import com.github.dtprj.dongting.dtkv.KvStatusResp;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.SimpleWritePacket;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.rpc.QueryStatusProcessor;
import com.github.dtprj.dongting.raft.rpc.RaftSequenceProcessor;
import com.github.dtprj.dongting.raft.rpc.ReqInfoEx;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

/**
 * @author huangli
 */
class KvStatusProcessor extends RaftSequenceProcessor<Integer> {

    public KvStatusProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    @Override
    public DecoderCallback<Integer> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(context.cachedPbIntCallback());
    }

    @Override
    protected int getGroupId(ReadPacket<Integer> frame) {
        return frame.getBody();
    }

    @Override
    protected FiberFrame<Void> processInFiberGroup(ReqInfoEx<Integer> reqInfo) {
        DtKV kv = KvServerUtil.getStateMachine(reqInfo);
        if (kv == null) {
            return FiberFrame.voidCompletedFrame();
        }
        RaftStatusImpl raftStatus = reqInfo.raftGroup.groupComponents.raftStatus;
        KvStatusResp resp = new KvStatusResp();
        resp.raftServerStatus = QueryStatusProcessor.buildQueryStatusResp(raftServer.getServerConfig().nodeId, raftStatus);
        if (kv.useSeparateExecutor) {
            // assert kv.dtkvExecutor != null;
            //noinspection DataFlowIssue
            kv.dtkvExecutor.execute(() -> finishAndWriteResp(kv, resp, reqInfo));
        } else {
            finishAndWriteResp(kv, resp, reqInfo);
        }
        return FiberFrame.voidCompletedFrame();
    }

    private void finishAndWriteResp(DtKV kv, KvStatusResp resp, ReqInfo<?> reqInfo) {
        resp.watchCount = kv.watchManager.updateWatchStatus(reqInfo.reqContext.getDtChannel());
        SimpleWritePacket wf = new SimpleWritePacket(resp);
        wf.setRespCode(CmdCodes.SUCCESS);
        reqInfo.reqContext.writeRespInBizThreads(wf);
    }
}

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
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.DtChannel;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.util.concurrent.RejectedExecutionException;

/**
 * @author huangli
 */
final class WatchProcessor extends RaftProcessor<WatchReqCallback> {

    public WatchProcessor(RaftServer raftServer) {
        super(raftServer);
    }

    @Override
    protected int getGroupId(ReadPacket<WatchReqCallback> frame) {
        return frame.getBody().groupId;
    }

    @Override
    public DecoderCallback<WatchReqCallback> createDecoderCallback(int command, DecodeContext context) {
        return context.toDecoderCallback(new WatchReqCallback());
    }

    @Override
    protected WritePacket doProcess(ReqInfo<WatchReqCallback> reqInfo) {
        DtKV dtKV = KvServerUtil.getStateMachine(reqInfo);
        if (dtKV == null) {
            return null;
        }

        WatchReqCallback req = reqInfo.reqFrame.getBody();
        if ((!req.syncAll && (req.keys == null || req.keys.length == 0))
                || (req.keys != null && (req.knownRaftIndexes == null || req.knownRaftIndexes.length != req.keys.length))) {
            EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            p.setBizCode(KvCodes.CODE_CLIENT_REQ_ERROR);
            return p;
        }
        KvStatus kvStatus = dtKV.kvStatus;
        if (kvStatus.installSnapshot) {
            EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            p.setBizCode(KvCodes.CODE_INSTALL_SNAPSHOT);
            return p;
        }
        KvImpl kv = kvStatus.kvImpl;
        if (req.keys != null) {
            for (ByteArray key : req.keys) {
                int bc = kv.checkKey(key, false, true);
                if (bc != KvCodes.CODE_SUCCESS) {
                    EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
                    p.setBizCode(KvCodes.CODE_INVALID_KEY);
                    return p;
                }
            }
        }

        try {
            dtKV.submitTask(() -> exec(dtKV, reqInfo, req));
            return null;
        } catch (RejectedExecutionException ignore) {
            return new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_STOPPED);
        }
    }

    private void exec(DtKV dtKV, ReqInfo<WatchReqCallback> reqInfo, WatchReqCallback req) {
        KvStatus kvStatus = dtKV.kvStatus;
        if (kvStatus.installSnapshot) {
            EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            p.setBizCode(KvCodes.CODE_INSTALL_SNAPSHOT);
            reqInfo.reqContext.writeRespInBizThreads(p);
            return;
        }
        DtChannel channel = reqInfo.reqContext.getDtChannel();
        try {
            dtKV.watchManager.sync(kvStatus.kvImpl, channel, req.syncAll, req.keys, req.knownRaftIndexes);
        } catch (Exception e) {
            BugLog.log(e);
            EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
            reqInfo.reqContext.writeRespInBizThreads(p);
            return;
        }
        EmptyBodyRespPacket p = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
        p.setBizCode(KvCodes.CODE_SUCCESS);
        reqInfo.reqContext.writeRespInBizThreads(p);
    }

}

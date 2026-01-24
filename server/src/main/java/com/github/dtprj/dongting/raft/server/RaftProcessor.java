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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.FlowControlException;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.RaftTimeoutException;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftShareStatus;
import com.github.dtprj.dongting.raft.rpc.ReqInfoEx;

import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public abstract class RaftProcessor<T> extends ReqProcessor<T> {
    private static final DtLog log = DtLogs.getLogger(RaftProcessor.class);

    protected final RaftServer raftServer;
    private final boolean enableServicePort;
    private final boolean enableReplicatePort;

    public RaftProcessor(RaftServer raftServer, boolean enableServicePort, boolean enableReplicatePort) {
        this.raftServer = raftServer;
        this.enableServicePort = enableServicePort;
        this.enableReplicatePort = enableReplicatePort;
    }

    protected abstract int getGroupId(ReadPacket<T> frame);

    public static boolean requestServicePort(ReqContext reqContext, RaftServerConfig config) {
        return reqContext.getDtChannel().getLocalPort() == config.servicePort;
    }

    public static boolean checkPort(boolean requestServicePort, boolean enableServicePort, boolean enableReplicatePort) {
        if (requestServicePort) {
            return enableServicePort;
        } else {
            return enableReplicatePort;
        }
    }

    public static WritePacket createWrongPortRest(ReadPacket<?> packet, ReqContext reqContext) {
        EmptyBodyRespPacket errorResp = new EmptyBodyRespPacket(CmdCodes.COMMAND_NOT_SUPPORT);
        errorResp.msg = "command not supported on this port";
        log.info("command not supported on this port: cmd={}, channel={}",
                packet.command, reqContext.getDtChannel());
        return errorResp;
    }

    /**
     * run in io thread.
     */
    @Override
    public final WritePacket process(ReadPacket<T> packet, ReqContext reqContext) {
        if (packet.getBody() == null) {
            EmptyBodyRespPacket errorResp = new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR);
            errorResp.msg = "request has no body";
            log.warn("request has no body: cmd={}, channel={}", packet.command, reqContext.getDtChannel());
            return errorResp;
        }
        boolean servicePort = requestServicePort(reqContext, raftServer.getServerConfig());
        if (!checkPort(servicePort, enableServicePort, enableReplicatePort)) {
            packet.clean();
            return createWrongPortRest(packet, reqContext);
        }
        if (servicePort) {
            if (!raftServer.isGroupReady()) {
                packet.clean();
                EmptyBodyRespPacket wf = new EmptyBodyRespPacket(CmdCodes.NOT_INIT);
                wf.msg = "raft server not ready";
                return wf;
            }
            if (raftServer.getStatus() > AbstractLifeCircle.STATUS_RUNNING) {
                packet.clean();
                EmptyBodyRespPacket wf = new EmptyBodyRespPacket(CmdCodes.STOPPING);
                wf.msg = "raft server shutdown";
                return wf;
            }
        }
        int groupId = getGroupId(packet);
        RaftGroupImpl g = (RaftGroupImpl) raftServer.getRaftGroup(groupId);
        if (g == null) {
            packet.clean();
            EmptyBodyRespPacket errorResp = new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_NOT_FOUND);
            errorResp.msg = "raft group not found: " + groupId;
            log.error(errorResp.msg);
            return errorResp;
        }
        ReqInfoEx<T> reqInfo = new ReqInfoEx<>(packet, reqContext, g);
        RaftShareStatus ss = g.groupComponents.raftStatus.getShareStatus();
        if (!ss.initFinished && packet.command != Commands.RAFT_QUERY_STATUS) {
            // raft query status can be processed when not initialized
            packet.clean();
            EmptyBodyRespPacket wf = new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_NOT_INIT);
            wf.msg = "raft group not initialized: " + groupId;
            return wf;
        }

        if (ss.shouldStop) {
            packet.clean();
            return createStoppedResp(groupId);
        }
        // release in subclass
        return doProcess(reqInfo);
    }

    protected abstract WritePacket doProcess(ReqInfo<T> reqInfo);

    protected void writeErrorResp(ReqInfo<?> reqInfo, Throwable ex) {
        Throwable root = DtUtil.rootCause(ex);
        if (root instanceof RaftTimeoutException) {
            ReadPacket<?> reqFrame = reqInfo.reqFrame;
            log.warn("raft operation timeout: command={}, seq={}", reqFrame.command, reqFrame.seq);
            return;
        }
        EmptyBodyRespPacket errorResp;
        if (root instanceof FlowControlException) {
            errorResp = new EmptyBodyRespPacket(CmdCodes.FLOW_CONTROL);
        } else if (root instanceof NotLeaderException) {
            errorResp = new EmptyBodyRespPacket(CmdCodes.NOT_RAFT_LEADER);
            RaftNode leader = ((NotLeaderException) root).getCurrentLeader();
            if (leader != null) {
                errorResp.extra = String.valueOf(leader.nodeId).getBytes(StandardCharsets.UTF_8);
            }
            log.warn("not leader, current leader is {}", leader);
        } else if (root.getMessage().contains("the fiber group is not running")) {
            errorResp = new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_STOPPED);
        } else {
            errorResp = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
            log.warn("raft processor error", ex);
        }
        errorResp.msg = root.toString();
        reqInfo.reqContext.writeRespInBizThreads(errorResp);
    }

    protected EmptyBodyRespPacket createStoppedResp(int groupId) {
        EmptyBodyRespPacket wf = new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_STOPPED);
        wf.msg = "raft group is stopped: " + groupId;
        return wf;
    }
}

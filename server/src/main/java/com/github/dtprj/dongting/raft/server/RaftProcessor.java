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
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WritePacket;
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

    public static WritePacket createWrongPortResp(ReadPacket<?> packet, ReqContext reqContext) {
        log.info("command not supported on this port: cmd={}, channel={}",
                packet.command, reqContext.getDtChannel());
        return new EmptyBodyRespPacket(CmdCodes.COMMAND_NOT_SUPPORT, "command not supported on this port");
    }

    /**
     * run in io thread.
     */
    @Override
    public final WritePacket process(ReadPacket<T> packet, ReqContext reqContext) {
        if (packet.getBody() == null) {
            log.warn("request has no body: cmd={}, channel={}", packet.command, reqContext.getDtChannel());
            return new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR, "request has no body");
        }
        boolean servicePort = requestServicePort(reqContext, raftServer.getServerConfig());
        if (!checkPort(servicePort, enableServicePort, enableReplicatePort)) {
            packet.clean();
            return createWrongPortResp(packet, reqContext);
        }
        if (servicePort) {
            if (!raftServer.isGroupReady()) {
                packet.clean();
                return new EmptyBodyRespPacket(CmdCodes.NOT_INIT, "raft server not ready");
            }
            if (raftServer.getStatus() > AbstractLifeCircle.STATUS_RUNNING) {
                packet.clean();
                return new EmptyBodyRespPacket(CmdCodes.STOPPING, "raft server shutdown");
            }
        }
        int groupId = getGroupId(packet);
        if (groupId <= 0) {
            packet.clean();
            return new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR, "groupId must be positive");
        }
        RaftGroupImpl g = (RaftGroupImpl) raftServer.getRaftGroup(groupId);
        if (g == null) {
            packet.clean();
            log.error("raft group not found: {}", groupId);
            return new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_NOT_FOUND,
                    "raft group not found: " + groupId);
        }
        ReqInfoEx<T> reqInfo = new ReqInfoEx<>(packet, reqContext, g);
        RaftShareStatus ss = g.groupComponents.raftStatus.getShareStatus();
        if (ss.fatalError && !isQueryStatus(packet.command)) {
            packet.clean();
            return new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_ERROR,
                    "raft group in error status: " + groupId);
        }
        if (!ss.initFinished && !isQueryStatus(packet.command)) {
            // raft query status can be processed when not initialized
            packet.clean();
            return new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_NOT_INIT,
                    "raft group not initialized: " + groupId);
        }

        if (ss.shouldStop && !isQueryStatus(packet.command)) {
            packet.clean();
            return createStoppedResp(groupId);
        }
        // release in subclass
        return doProcess(reqInfo);
    }

    protected boolean isQueryStatus(int cmd) {
        return cmd == Commands.RAFT_QUERY_STATUS || cmd == Commands.DTKV_QUERY_STATUS;
    }

    protected abstract WritePacket doProcess(ReqInfo<T> reqInfo);

    protected void writeErrorResp(ReqInfo<?> reqInfo, Throwable ex) {
        Throwable root = DtUtil.rootCause(ex);
        if (root instanceof RaftTimeoutException) {
            ReadPacket<?> reqFrame = reqInfo.reqFrame;
            log.warn("raft operation timeout: command={}, seq={}", reqFrame.command, reqFrame.seq);
            return;
        }
        int code;
        byte[] extra = null;
        if (root instanceof FlowControlException) {
            code = CmdCodes.FLOW_CONTROL;
        } else if (root instanceof NotLeaderException) {
            code = CmdCodes.NOT_RAFT_LEADER;
            int leaderId = ((NotLeaderException) root).getCurrentLeaderId();
            if (leaderId > 0) {
                extra = String.valueOf(leaderId).getBytes(StandardCharsets.UTF_8);
            }
            log.warn("not leader, current leader is {}", leaderId);
        } else if (root.getMessage() != null && root.getMessage().contains("the fiber group is not running")) {
            code = CmdCodes.RAFT_GROUP_STOPPED;
        } else if (root instanceof NetCodeException) {
            NetCodeException nce = (NetCodeException) root;
            code = nce.getCode();
            extra = nce.getExtra();
        } else {
            code = CmdCodes.SYS_ERROR;
            log.warn("raft processor error", ex);
        }
        EmptyBodyRespPacket errorResp = new EmptyBodyRespPacket(code, root);
        errorResp.extra = extra;
        reqInfo.reqContext.writeRespInBizThreads(errorResp);
    }

    protected EmptyBodyRespPacket createStoppedResp(int groupId) {
        return new EmptyBodyRespPacket(CmdCodes.RAFT_GROUP_STOPPED, "raft group is stopped: " + groupId);
    }
}

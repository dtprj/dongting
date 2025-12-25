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
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftNode;
import com.github.dtprj.dongting.raft.impl.NodeManager;
import com.github.dtprj.dongting.raft.server.RaftFactory;
import com.github.dtprj.dongting.raft.server.RaftGroupConfig;
import com.github.dtprj.dongting.raft.server.RaftServer;

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AdminGroupAndNodeProcessor extends ReqProcessor<Object> {

    private final RaftServer server;
    private final RaftFactory factory;
    private final NodeManager nodeManager;

    public AdminGroupAndNodeProcessor(RaftServer server, NodeManager nodeManager, RaftFactory factory) {
        this.server = server;
        this.factory = factory;
        this.nodeManager = nodeManager;
    }

    @Override
    public WritePacket process(ReadPacket<Object> packet, ReqContext reqContext) throws Exception {
        boolean servicePort = RaftPingProcessor.requestServicePort(reqContext, server.getServerConfig());
        if (!RaftPingProcessor.checkPort(servicePort, false, true)) {
            packet.clean();
            return RaftPingProcessor.createWrongPortRest(packet, reqContext);
        }
        int cmd = packet.command;
        switch (cmd) {
            case Commands.RAFT_ADMIN_ADD_GROUP:
                addGroup((AdminAddGroupReq) packet.getBody(), reqContext);
                break;
            case Commands.RAFT_ADMIN_REMOVE_GROUP:
                removeGroup((Integer) packet.getBody(), reqContext);
                break;
            case Commands.RAFT_ADMIN_ADD_NODE:
                addNode((AdminAddNodeReq) packet.getBody(), reqContext);
                break;
            case Commands.RAFT_ADMIN_REMOVE_NODE:
                removeNode((Integer) packet.getBody(), reqContext);
                break;
            case Commands.RAFT_ADMIN_LIST_NODES:
                return listNodes();
            case Commands.RAFT_ADMIN_LIST_GROUPS:
                return listGroups();
            default:
                throw new RaftException("bad cmd:" + cmd);
        }
        return null;
    }


    @Override
    public DecoderCallback createDecoderCallback(int command, DecodeContext context) {
        switch (command) {
            case Commands.RAFT_ADMIN_ADD_GROUP:
                return context.toDecoderCallback(new AdminAddGroupReq());
            case Commands.RAFT_ADMIN_ADD_NODE:
                return context.toDecoderCallback(new AdminAddNodeReq());
            case Commands.RAFT_ADMIN_REMOVE_GROUP:
            case Commands.RAFT_ADMIN_REMOVE_NODE:
                return context.toDecoderCallback(context.cachedPbIntCallback());
            case Commands.RAFT_ADMIN_LIST_NODES:
            case Commands.RAFT_ADMIN_LIST_GROUPS:
                return DecoderCallback.VOID_DECODE_CALLBACK;
            default:
                throw new RaftException("bad command:" + command);
        }
    }

    private void processResult(CompletableFuture<?> f, ReqContext reqContext) {
        f.whenComplete((o, ex) -> {
            EmptyBodyRespPacket resp;
            if (ex == null) {
                resp = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
            } else {
                resp = new EmptyBodyRespPacket(CmdCodes.SYS_ERROR);
                resp.msg = ex.toString();
            }
            reqContext.writeRespInBizThreads(resp);
        });
    }

    private void addGroup(AdminAddGroupReq req, ReqContext reqContext) {
        RaftGroupConfig c = factory.createConfig(req.groupId, req.nodeIdOfMembers, req.nodeIdOfObservers);
        CompletableFuture<Void> f = server.addGroup(c);
        processResult(f, reqContext);
    }

    private void removeGroup(Integer groupId, ReqContext reqContext) {
        CompletableFuture<Void> f = server.removeGroup(groupId, true, reqContext.getTimeout());
        processResult(f, reqContext);
    }

    private void addNode(AdminAddNodeReq req, ReqContext reqContext) {
        RaftNode n = new RaftNode(req.nodeId, new HostPort(req.host, req.port));
        CompletableFuture<Void> f = server.addNode(n);
        processResult(f, reqContext);
    }

    private void removeNode(Integer nodeId, ReqContext reqContext) {
        CompletableFuture<Void> f = server.removeNode(nodeId);
        processResult(f, reqContext);
    }

    private WritePacket listNodes() {
        AdminListNodesResp resp = new AdminListNodesResp();
        resp.nodes = nodeManager.getAllNodes();
        return new EncodableBodyWritePacket(resp);
    }

    private WritePacket listGroups() {
        AdminListGroupsResp resp = new AdminListGroupsResp();
        resp.groupIds = server.getAllGroupIds();
        return new EncodableBodyWritePacket(resp);
    }
}

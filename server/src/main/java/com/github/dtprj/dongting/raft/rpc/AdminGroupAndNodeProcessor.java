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
import com.github.dtprj.dongting.codec.PbIntCallback;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.ReqContext;
import com.github.dtprj.dongting.net.ReqProcessor;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.RaftNode;
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

    public AdminGroupAndNodeProcessor(RaftServer server, RaftFactory factory) {
        this.server = server;
        this.factory = factory;
    }

    @Override
    public WritePacket process(ReadPacket<Object> packet, ReqContext reqContext) throws Exception {
        int cmd = packet.getCommand();
        if (cmd == Commands.RAFT_ADMIN_ADD_GROUP) {
            AdminAddGroupReq req = (AdminAddGroupReq) packet.getBody();
            addGroup(req, packet, reqContext);
        } else if (cmd == Commands.RAFT_ADMIN_REMOVE_GROUP) {
            Integer groupId = (Integer) packet.getBody();
            removeGroup(groupId, packet, reqContext);
        } else if (cmd == Commands.RAFT_ADMIN_ADD_NODE) {
            AdminAddNodeReq req = (AdminAddNodeReq) packet.getBody();
            addNode(req, packet, reqContext);
        } else if (cmd == Commands.RAFT_ADMIN_REMOVE_NODE) {
            Integer nodeId = (Integer) packet.getBody();
            removeNode(nodeId, packet, reqContext);
        } else {
            throw new RaftException("bad cmd:" + cmd);
        }
        return null;
    }


    @Override
    public DecoderCallback createDecoderCallback(int command, DecodeContext context) {
        if (command == Commands.RAFT_ADMIN_ADD_GROUP) {
            return context.toDecoderCallback(new AdminAddGroupReq());
        } else if (command == Commands.RAFT_ADMIN_REMOVE_GROUP) {
            return context.toDecoderCallback(new PbIntCallback());
        } else if (command == Commands.RAFT_ADMIN_ADD_NODE) {
            return context.toDecoderCallback(new AdminAddNodeReq());
        } else if (command == Commands.RAFT_ADMIN_REMOVE_NODE) {
            return context.toDecoderCallback(new PbIntCallback());
        } else {
            throw new RaftException("bad command:" + command);
        }
    }

    private void processResult(CompletableFuture<?> f, ReadPacket<Object> packet, ReqContext reqContext) {
        f.whenComplete((o, ex) -> {
            if (ex == null) {
                EmptyBodyRespPacket resp = new EmptyBodyRespPacket(CmdCodes.SUCCESS);
                reqContext.getRespWriter().writeRespInBizThreads(packet, resp, reqContext.getTimeout());
            } else {
                EmptyBodyRespPacket resp = new EmptyBodyRespPacket(CmdCodes.BIZ_ERROR);
                resp.setMsg(ex.toString());
                reqContext.getRespWriter().writeRespInBizThreads(packet, resp, reqContext.getTimeout());
            }
        });
    }

    private void addGroup(AdminAddGroupReq req, ReadPacket<Object> packet, ReqContext reqContext) {
        RaftGroupConfig c = factory.createConfig(req.groupId, req.nodeIdOfMembers, req.nodeIdOfObservers);
        CompletableFuture<Void> f = server.addGroup(c);
        processResult(f, packet, reqContext);
    }

    private void removeGroup(Integer groupId, ReadPacket<Object> packet, ReqContext reqContext) {
        CompletableFuture<Void> f = server.removeGroup(groupId, true, reqContext.getTimeout());
        processResult(f, packet, reqContext);
    }

    private void addNode(AdminAddNodeReq req, ReadPacket<Object> packet, ReqContext reqContext) {
        RaftNode n = new RaftNode(req.nodeId, new HostPort(req.host, req.port));
        CompletableFuture<Void> f = server.addNode(n);
        processResult(f, packet, reqContext);
    }

    private void removeNode(Integer nodeId, ReadPacket<Object> packet, ReqContext reqContext) {
        CompletableFuture<Void> f = server.removeNode(nodeId);
        processResult(f, packet, reqContext);
    }
}

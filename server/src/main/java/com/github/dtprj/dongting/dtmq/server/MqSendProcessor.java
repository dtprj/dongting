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
package com.github.dtprj.dongting.dtmq.server;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

final class MqSendProcessor extends RaftProcessor<MqSendReqCallback> {
    MqSendProcessor(RaftServer raftServer) {
        super(raftServer, true, false);
    }

    @Override
    public DecoderCallback<MqSendReqCallback> createDecoderCallback(int cmd, DecodeContext context) {
        return context.toDecoderCallback(new MqSendReqCallback());
    }

    @Override
    protected int getGroupId(MqSendReqCallback body) {
        return body.groupId;
    }

    @Override
    protected WritePacket doProcess(ReqInfo<MqSendReqCallback> reqInfo) {
        MqSendReqCallback body = reqInfo.reqFrame.getBody();
        boolean submitted = false;
        try {
            DtMQ mq = MqServerUtil.getStateMachine(reqInfo);
            if (mq == null) {
                return null;
            }
            RaftReqData data = body.data;
            if (data.bizKey <= 0) {
                return new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR, "queueId must be positive");
            }
            int maxHeaderSize = mq.serverConfig.maxHeaderSize;
            if (data.bizHeaderLen > maxHeaderSize) {
                return new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR,
                        "message header exceeds maxHeaderSize: " + maxHeaderSize);
            }
            int maxBodySize = mq.serverConfig.maxBodySize;
            if (data.bodyLen > maxBodySize) {
                return new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR,
                        "message body exceeds maxBodySize: " + maxBodySize);
            }
            reqInfo.raftGroup.submitLinearTask(new SendTask(reqInfo, data)); // RefBuffer ownership changed
            submitted = true;
        } catch (Exception e) {
            writeErrorResp(reqInfo, e);
        } finally {
            if (!submitted) {
                reqInfo.reqFrame.clean(); // call ReadPacket<MqSendReqCallback>.clean() and release MqSendReqCallback.data
            }
            body.data = null;
        }
        return null;
    }

    private final class SendTask extends RaftTask implements RaftCallback {
        private ReqInfo<MqSendReqCallback> reqInfo;

        private SendTask(ReqInfo<MqSendReqCallback> reqInfo, RaftReqData data) {
            super(data, null, null, reqInfo.reqContext.getTimeout(), false, null);
            this.reqInfo = reqInfo;
            this.callback = this;
        }

        @Override
        public void success(long raftIndex, Object result) {
            try {
                reqInfo.reqContext.writeRespInBizThreads(new EmptyBodyRespPacket(CmdCodes.SUCCESS));
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

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
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.NetCodeException;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.WritePacket;
import com.github.dtprj.dongting.raft.impl.DecodeContextEx;
import com.github.dtprj.dongting.raft.impl.RaftTask;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftProcessor;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;

import java.nio.ByteBuffer;
import java.util.ArrayList;

final class MqSendProcessor extends RaftProcessor<MqSendReq> {
    MqSendProcessor(RaftServer raftServer) {
        super(raftServer, true, false);
    }

    @Override
    public DecoderCallback<MqSendReq> createDecoderCallback(int cmd, DecodeContext context) {
        return context.toDecoderCallback(((DecodeContextEx) context).mqSendReqCallback());
    }

    @Override
    protected int getGroupId(ReadPacket<MqSendReq> packet) {
        int groupId = packet.getBody().groupId;
        if (groupId <= 0) {
            packet.clean();
            throw new NetCodeException(CmdCodes.CLIENT_ERROR, "groupId must be positive", null);
        }
        return groupId;
    }

    @Override
    protected WritePacket doProcess(ReqInfo<MqSendReq> reqInfo) {
        try {
            if (!(reqInfo.raftGroup.getStateMachine() instanceof DtMQ)) {
                throw new NetCodeException(CmdCodes.CLIENT_ERROR, "not a dtmq group", null);
            }
            DtMQ mq = (DtMQ) reqInfo.raftGroup.getStateMachine();
            ArrayList<RaftReqData> messages = reqInfo.reqFrame.getBody().messages;
            if (messages == null || messages.isEmpty()) {
                throw new NetCodeException(CmdCodes.CLIENT_ERROR, "no messages", null);
            }
            int size = messages.size();
            int maxBody = mq.serverConfig.maxBody;
            for (int i = 0; i < size; i++) {
                if (messages.get(i).bodyLen > maxBody) {
                    throw new NetCodeException(CmdCodes.CLIENT_ERROR, "message body exceeds maxBody: " + maxBody, null);
                }
            }
            SendBatch batch = new SendBatch(reqInfo, size);
            for (int i = 0; i < size; i++) {
                SendTask task = new SendTask(batch, messages.get(i), i);
                messages.set(i, null);
                reqInfo.raftGroup.submitLinearTask(task);
            }
        } catch (Exception e) {
            writeErrorResp(reqInfo, e);
        } finally {
            reqInfo.reqFrame.clean();
        }
        return null;
    }

    private final class SendBatch implements Encodable {
        private ReqInfo<MqSendReq> reqInfo;
        private final long[] raftIndexes;
        private int remaining;
        private Throwable failure;

        private SendBatch(ReqInfo<MqSendReq> reqInfo, int size) {
            this.reqInfo = reqInfo;
            this.raftIndexes = new long[size];
            this.remaining = size;
        }

        // Rejected submissions can call back on the IO thread while other tasks finish on the Raft thread.
        private synchronized void complete(int index, long raftIndex, Throwable ex) {
            raftIndexes[index] = raftIndex;
            if (ex != null && failure == null) {
                failure = ex;
            }
            if (--remaining != 0) {
                return;
            }
            try {
                if (failure == null) {
                    EncodableBodyWritePacket resp = new EncodableBodyWritePacket(this);
                    resp.respCode = CmdCodes.SUCCESS;
                    reqInfo.reqContext.writeRespInBizThreads(resp);
                } else {
                    writeErrorResp(reqInfo, failure);
                }
            } finally {
                reqInfo = null;
                failure = null;
            }
        }

        @Override
        public int actualSize() {
            return PbUtil.sizeOfFix64Field(1, raftIndexes);
        }

        @Override
        public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
            return EncodeUtil.encodeFix64s(context, destBuffer, 1, raftIndexes);
        }
    }

    private static final class SendTask extends RaftTask implements RaftCallback {
        private SendBatch batch;
        private final int index;

        private SendTask(SendBatch batch, RaftReqData data, int index) {
            super(data, null, null, batch.reqInfo.reqContext.getTimeout(), false, null);
            this.batch = batch;
            this.index = index;
            this.callback = this;
        }

        @Override
        public void success(long raftIndex, Object result) {
            try {
                batch.complete(index, raftIndex, null);
            } finally {
                batch = null;
            }
        }

        @Override
        public void fail(Throwable ex) {
            try {
                batch.complete(index, 0, ex);
            } finally {
                batch = null;
            }
        }
    }
}

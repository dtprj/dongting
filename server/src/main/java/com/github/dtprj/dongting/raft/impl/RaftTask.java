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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.raft.server.RaftCallback;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.server.RaftReqData;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class RaftTask extends RaftInput implements Encodable {

    long perfTime;

    public long raftLogPosition;
    public long localCreateNanos;

    private boolean invokeCallback;

    boolean addPending;

    public RaftTask(RaftReqData reqData, Object bizHeader, Object bizBody,
                    boolean readOnly) {
        super(reqData, bizHeader, bizBody, null, readOnly, null);
    }

    public RaftTask(RaftReqData reqData, Object bizHeader, Object bizBody, DtTime deadline,
                    boolean readOnly, RaftCallback callback) {
        super(reqData, bizHeader, bizBody, deadline, readOnly, callback);
    }

    public void init(long localCreateNanos) {
        this.localCreateNanos = localCreateNanos;
    }

    public void callSuccess(Object r) {
        if (!invokeCallback) {
            try {
                RaftCallback.callSuccess(callback, reqData.index, r);
            } finally {
                callback = null;
                invokeCallback = true;
            }
        }
    }

    public void callFail(Throwable ex) {
        if (!invokeCallback) {
            try {
                RaftCallback.callFail(callback, ex);
            } finally {
                callback = null;
                invokeCallback = true;
            }
        }
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        return reqData.buffer.encode(context, destBuffer);
    }

    @Override
    public int actualSize() {
        return reqData.totalLen;
    }
}

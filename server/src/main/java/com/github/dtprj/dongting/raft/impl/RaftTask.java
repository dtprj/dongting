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
import com.github.dtprj.dongting.raft.store.LogHeader;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class RaftTask extends RaftInput implements Encodable {

    static final int HEADER_FINISHED = 1;
    static final int BIZ_HEADER_FINISHED = 2;
    static final int BIZ_HEADER_CRC_FINISHED = 3;
    static final int BIZ_BODY_FINISHED = 4;
    static final int BIZ_BODY_CRC_FINISHED = 5;

    public long perfTime;

    public long localCreateNanos;

    public final int type;

    public int term;
    public int prevLogTerm;
    public long index;
    public long timestamp;

    private boolean invokeCallback;

    boolean addPending;

    public RaftTask(int type, int bizType, RaftReqData reqData, Object bizHeader, Object bizBody, DtTime deadline,
                    boolean readOnly, RaftCallback callback) {
        super(bizType, reqData, bizHeader, bizBody, deadline, readOnly, callback);
        this.type = type;
    }

    // TODO optimise RaftTask.init()
    public void init(int term, int prevLogTerm, long index, long timestamp, long localCreateNanos) {
        this.localCreateNanos = localCreateNanos;
        this.term = term;
        this.prevLogTerm = prevLogTerm;
        this.index = index;
        this.timestamp = timestamp;
    }

    public void callSuccess(Object r) {
        if (!invokeCallback) {
            try {
                RaftCallback.callSuccess(callback, index, r);
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
        EncodeContext sub = null;
        RaftReqData reqData = this.reqData;
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (destBuffer.remaining() < LogHeader.ITEM_HEADER_SIZE) {
                    return false;
                } else {
                    CRC32C crc = (CRC32C) context.status; // require the caller set the crc as status
                    LogHeader.writeHeader(crc, destBuffer, this);
                    context.stage = HEADER_FINISHED;
                }
                sub = context.createOrGetNestedContext(true);
                // fall through
            case HEADER_FINISHED:
                if (reqData.bizHeaderSize > 0) {
                    if (sub == null) {
                        sub = context.createOrGetNestedContext(false);
                    }
                    boolean finish = reqData.bizHeader.encode(sub, destBuffer);
                    if (finish) {
                        // the body's reset called by parent context.reset(),
                        // if error occurred, the sub context will be reset by parent context.reset()
                        sub.reset();
                        context.stage = BIZ_HEADER_FINISHED;
                    } else {
                        return false;
                    }
                }
                // fall through
            case BIZ_HEADER_FINISHED:
                if (reqData.bizHeaderSize > 0) {
                    if (destBuffer.remaining() < 4) {
                        return false;
                    } else {
                        destBuffer.putInt(reqData.bizHeaderCrc);
                    }
                }
                context.stage = BIZ_HEADER_CRC_FINISHED;
                // fall through
            case BIZ_HEADER_CRC_FINISHED:
                if (reqData.bizBodySize > 0) {
                    if (sub == null) {
                        sub = context.createOrGetNestedContext(false);
                    }
                    boolean finish = reqData.bizBody.encode(sub, destBuffer);
                    if (finish) {
                        context.stage = BIZ_BODY_FINISHED;
                    } else {
                        return false;
                    }
                }
                // fall through
            case BIZ_BODY_FINISHED:
                if (reqData.bizBodySize > 0) {
                    if (destBuffer.remaining() < 4) {
                        return false;
                    } else {
                        destBuffer.putInt(reqData.bizBodyCrc);
                    }
                }
                context.stage = BIZ_BODY_CRC_FINISHED;
        }
        return true;
    }

    @Override
    public int actualSize() {
        return LogHeader.ITEM_HEADER_SIZE + reqData.totalSize;
    }
}

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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.RefBufferDecoderCallback;
import com.github.dtprj.dongting.common.RefCount;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.server.RaftReqData;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
// re-used
class LogItemCallback extends PbCallback<Object> {

    private LogItem item;
    private RefBuffer bizHeader;
    private RefBuffer bizBody;
    RaftCodecFactory codecFactory;
    private final DecoderCallback<RefBuffer> refBufferCallback = new RefBufferDecoderCallback();

    public LogItemCallback() {
    }

    @Override
    protected void begin(int len) {
        item = new LogItem();
    }

    @Override
    protected void end(boolean success) {
        if (!success) {
            if (bizHeader != null && bizHeader instanceof RefCount) {
                ((RefCount) bizHeader).release();
            }
            if (bizBody != null && bizBody instanceof RefCount) {
                ((RefCount) bizBody).release();
            }
        }
        item = null;
        bizHeader = null;
        bizBody = null;
    }

    @Override
    protected Object getResult() {
        item.reqData = new RaftReqData(bizHeader, 0, bizBody, 0);
        return item;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case LogItem.IDX_TYPE:
                item.type = (int) value;
                break;
            case LogItem.IDX_BIZ_TYPE:
                item.bizType = (int) value;
                break;
            case LogItem.IDX_TERM:
                item.term = (int) value;
                break;
            case LogItem.IDX_PREV_LOG_TERM:
                item.prevLogTerm = (int) value;
                break;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        switch (index) {
            case LogItem.IDX_INDEX:
                item.index = value;
                break;
            case LogItem.IDX_TIMESTAMP:
                item.timestamp = value;
                break;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int len, int currentPos) {
        boolean end = buf.remaining() >= len - currentPos;
        if (index == LogItem.IDX_HEADER) {
            RefBuffer result = parseNested(buf, len, currentPos, refBufferCallback);
            if (end) {
                bizHeader = result;
            }
        } else if (index == LogItem.IDX_BODY) {
            RefBuffer result = parseNested(buf, len, currentPos, refBufferCallback);
            if (end) {
                bizBody = result;
            }
        }
        return true;
    }
}

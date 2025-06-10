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

import com.github.dtprj.dongting.codec.DecoderCallback;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.raft.server.LogItem;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
// re-used
class LogItemCallback extends PbCallback<Object> {

    private LogItem item;
    RaftCodecFactory codecFactory;

    @Override
    protected void begin(int len) {
        item = new LogItem();
    }

    @Override
    protected boolean end(boolean success) {
        if (!success) {
            item.release();
        }
        item = null;
        return success;
    }

    @Override
    protected Object getResult() {
        return item;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case LogItem.IDX_TYPE:
                item.setType((int) value);
                break;
            case LogItem.IDX_BIZ_TYPE:
                item.setBizType((int) value);
                break;
            case LogItem.IDX_TERM:
                item.setTerm((int) value);
                break;
            case LogItem.IDX_PREV_LOG_TERM:
                item.setPrevLogTerm((int) value);
                break;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        switch (index) {
            case LogItem.IDX_INDEX:
                item.setIndex(value);
                break;
            case LogItem.IDX_TIMESTAMP:
                item.setTimestamp(value);
                break;
        }
        return true;
    }

    @Override
    public boolean readBytes(int index, ByteBuffer buf, int len, int currentPos) {
        boolean begin = currentPos == 0;
        boolean end = buf.remaining() >= len - currentPos;
        DecoderCallback<? extends Encodable> currentDecoderCallback;
        if (index == LogItem.IDX_HEADER) {
            if (begin) {
                item.setActualHeaderSize(len);
                if (item.getType() == LogItem.TYPE_NORMAL) {
                    currentDecoderCallback = codecFactory.createHeaderCallback(item.getBizType(), context.createOrGetNestedContext());
                } else {
                    currentDecoderCallback = new ByteArray.Callback();
                }
            } else {
                currentDecoderCallback = null;
            }
            Encodable result = parseNested(buf, len, currentPos, currentDecoderCallback);
            if (end) {
                item.setHeader(result);
            }
        } else if (index == LogItem.IDX_BODY) {
            if (begin) {
                item.setActualBodySize(len);
                if (item.getType() == LogItem.TYPE_NORMAL || item.getType() == LogItem.TYPE_LOG_READ) {
                    currentDecoderCallback = codecFactory.createBodyCallback(item.getBizType(), context.createOrGetNestedContext());
                } else {
                    currentDecoderCallback = new ByteArray.Callback();
                }
            } else {
                currentDecoderCallback = null;
            }
            Encodable result = parseNested(buf, len, currentPos, currentDecoderCallback);
            if (end) {
                item.setBody(result);
            }
        }
        return true;
    }
}

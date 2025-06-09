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
package com.github.dtprj.dongting.dtkv;

import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class WatchNotify implements Encodable {
    private static final int IDX_RAFT_INDEX = 1;
    private static final int IDX_STATE = 2;
    private static final int IDX_KEY = 3;
    private static final int IDX_VALUE = 4;

    public final long raftIndex;
    public final int state;
    public final byte[] key;
    public final byte[] value;

    public WatchNotify(long raftIndex, int state, byte[] key, byte[] value) {
        this.raftIndex = raftIndex;
        this.state = state;
        this.key = key;
        this.value = value;
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfFix64Field(IDX_RAFT_INDEX, raftIndex)
                + PbUtil.sizeOfInt32Field(IDX_STATE, state)
                + EncodeUtil.sizeOf(IDX_KEY, key)
                + EncodeUtil.sizeOf(IDX_VALUE, value);
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_RAFT_INDEX, raftIndex)) {
                    return false;
                }
                // fall through
            case IDX_RAFT_INDEX:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_STATE, state)) {
                    return false;
                }
                // fall through
            case IDX_STATE:
                if (key != null && !EncodeUtil.encode(context, destBuffer, IDX_KEY, key)) {
                    return false;
                }
                // fall through
            case IDX_KEY:
                return value == null || EncodeUtil.encode(context, destBuffer, IDX_VALUE, value);
            default:
                throw new CodecException(context);
        }
    }

    // re-used
    public static class Callback extends PbCallback<WatchNotify> {
        private long raftIndex;
        private int state;
        private byte[] key;
        private byte[] value;

        @Override
        protected boolean end(boolean success) {
            raftIndex = 0;
            state = 0;
            key = null;
            value = null;
            return success;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == IDX_STATE) {
                state = (int) value;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            if (index == IDX_RAFT_INDEX) {
                raftIndex = value;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == IDX_KEY) {
                key = parseBytes(buf, fieldLen, currentPos);
            } else if (index == IDX_VALUE) {
                value = parseBytes(buf, fieldLen, currentPos);
            }
            return true;
        }

        @Override
        protected WatchNotify getResult() {
            return new WatchNotify(raftIndex, state, key, value);
        }
    }
}

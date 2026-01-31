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
public class KvNode implements Encodable {

    private static final int IDX_CREATE_INDEX = 1;
    private static final int IDX_CREATE_TIME = 2;
    private static final int IDX_UPDATE_INDEX = 3;
    private static final int IDX_UPDATE_TIME = 4;
    private static final int IDX_FLAG = 5;
    private static final int IDX_DATA = 15;

    public final long createIndex;
    public final long createTime;
    public final long updateIndex;
    public final long updateTime;

    public final byte[] data;

    public final int flag;
    public static final int FLAG_DIR_MASK = 0x1;
    public static final int FLAG_LOCK_MASK = 0x2;
    public static final int FLAG_TEMP_MASK = 0x4;

    private int encodeSize;

    public KvNode(long createIndex, long createTime, long updateIndex, long updateTime, int flag, byte[] data) {
        this.createIndex = createIndex;
        this.createTime = createTime;
        this.updateIndex = updateIndex;
        this.updateTime = updateTime;
        this.flag = flag;
        this.data = data;
    }

    /**
     * NOTICE: the lock node also has dir flag.
     */
    public final boolean isDir() {
        return (flag & FLAG_DIR_MASK) != 0;
    }

    /**
     * NOTICE: the lock node also has dir flag.
     */
    public final boolean isLock() {
        return (flag & FLAG_LOCK_MASK) != 0;
    }

    public final boolean isTemp() {
        return (flag & FLAG_TEMP_MASK) != 0;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_CREATE_INDEX, createIndex)) {
                    return false;
                }
                // fall through
            case IDX_CREATE_INDEX:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_CREATE_TIME, createTime)) {
                    return false;
                }
                // fall through
            case IDX_CREATE_TIME:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_UPDATE_INDEX, updateIndex)) {
                    return false;
                }
                // fall through
            case IDX_UPDATE_INDEX:
                if (!EncodeUtil.encodeFix64(context, destBuffer, IDX_UPDATE_TIME, updateTime)) {
                    return false;
                }
                // fall through
            case IDX_UPDATE_TIME:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_FLAG, flag)) {
                    return false;
                }
                // fall through
            case IDX_FLAG:
                return data == null || EncodeUtil.encode(context, destBuffer, IDX_DATA, data);
            default:
                throw new CodecException(context);

        }
    }

    @Override
    public int actualSize() {
        if (encodeSize == 0) {
            encodeSize = PbUtil.sizeOfFix64Field(IDX_CREATE_INDEX, createIndex)
                    + PbUtil.sizeOfFix64Field(IDX_CREATE_TIME, createTime)
                    + PbUtil.sizeOfFix64Field(IDX_UPDATE_INDEX, updateIndex)
                    + PbUtil.sizeOfFix64Field(IDX_UPDATE_TIME, updateTime)
                    + PbUtil.sizeOfInt32Field(IDX_FLAG, flag)
                    + EncodeUtil.sizeOf(IDX_DATA, data);
        }
        return encodeSize;
    }

    // re-used
    public static class Callback extends PbCallback<KvNode> {

        private long createIndex;
        private long createTime;
        private long updateIndex;
        private long updateTime;
        private int flag;
        private byte[] data;

        @Override
        protected void end(boolean success) {
            createIndex = 0;
            createTime = 0;
            updateIndex = 0;
            updateTime = 0;
            flag = 0;
            data = null;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == IDX_FLAG) {
                flag = (int) value;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case IDX_CREATE_INDEX:
                    createIndex = value;
                    break;
                case IDX_CREATE_TIME:
                    createTime = value;
                    break;
                case IDX_UPDATE_INDEX:
                    updateIndex = value;
                    break;
                case IDX_UPDATE_TIME:
                    updateTime = value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == IDX_DATA) {
                data = parseBytes(buf, fieldLen, currentPos);
            }
            return true;
        }

        @Override
        protected KvNode getResult() {
            return new KvNode(createIndex, createTime, updateIndex, updateTime, flag, data);
        }
    }
}

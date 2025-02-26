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
    private static final int IDX_DATA = 15;

    protected final long createIndex;
    protected final long createTime;
    protected final long updateIndex;
    protected final long updateTime;

    private final int headerSize;

    protected boolean dir;
    protected final byte[] data;

    public KvNode(long createIndex, long createTime, long updateIndex, long updateTime, boolean dir, byte[] data) {
        this.createIndex = createIndex;
        this.createTime = createTime;
        this.updateIndex = updateIndex;
        this.updateTime = updateTime;
        this.dir = dir;
        this.data = data;

        this.headerSize = PbUtil.sizeOfFix64Field(IDX_CREATE_INDEX, createIndex)
                + PbUtil.sizeOfFix64Field(IDX_CREATE_TIME, createTime)
                + PbUtil.sizeOfFix64Field(IDX_UPDATE_INDEX, updateIndex)
                + PbUtil.sizeOfFix64Field(IDX_UPDATE_TIME, updateTime);
    }

    public boolean isDir() {
        return dir;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        int remaining = destBuffer.remaining();
        if (context.stage == EncodeContext.STAGE_BEGIN) {
            if (remaining < headerSize) {
                return false;
            } else {
                PbUtil.writeFix64Field(destBuffer, IDX_CREATE_INDEX, createIndex);
                PbUtil.writeFix64Field(destBuffer, IDX_CREATE_TIME, createTime);
                PbUtil.writeFix64Field(destBuffer, IDX_UPDATE_INDEX, updateIndex);
                PbUtil.writeFix64Field(destBuffer, IDX_UPDATE_TIME, updateTime);
                context.stage = IDX_UPDATE_TIME;
            }
        }
        if (context.stage == IDX_UPDATE_TIME) {
            if (EncodeUtil.encodeBytes(context, destBuffer, IDX_DATA, data)) {
                context.stage = EncodeContext.STAGE_END;
                return true;
            } else {
                return false;
            }
        }

        throw new CodecException(context);
    }

    @Override
    public int actualSize() {
        return EncodeUtil.sizeOfBytesField(IDX_DATA, data) + headerSize;
    }

    // re-used
    public static class Callback extends PbCallback<KvNode> {

        private long createIndex;
        private long createTime;
        private long updateIndex;
        private long updateTime;
        private byte[] data;

        @Override
        protected boolean end(boolean success) {
            createIndex = 0;
            createTime = 0;
            updateIndex = 0;
            updateTime = 0;
            data = null;
            return success;
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
            return new KvNode(createIndex, createTime, updateIndex, updateTime, data == null || data.length == 0, data);
        }
    }

    public byte[] getData() {
        return data;
    }

    public long getCreateIndex() {
        return createIndex;
    }

    public long getUpdateIndex() {
        return updateIndex;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }
}

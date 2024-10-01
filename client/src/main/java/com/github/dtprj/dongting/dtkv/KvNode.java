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

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class KvNode {

    protected final long createIndex;
    protected final long createTime;
    protected final long updateIndex;
    protected final long updateTime;

    protected final byte[] data;

    public KvNode(long createIndex, long createTime, long updateIndex, long updateTime, byte[] data) {
        this.createIndex = createIndex;
        this.createTime = createTime;
        this.updateIndex = updateIndex;
        this.updateTime = updateTime;
        this.data = data;
    }

    public boolean isDir() {
        return data == null || data.length == 0;
    }

    public static int calcActualSize(KvNode n) {
        return n == null ? 0 : PbUtil.accurateLengthDelimitedSize(2, n.data == null ? 0 : n.data.length)
                + PbUtil.accurateFix64Size(3, n.createIndex)
                + PbUtil.accurateFix64Size(4, n.createTime)
                + PbUtil.accurateFix64Size(5, n.updateIndex)
                + PbUtil.accurateFix64Size(6, n.updateTime);
    }

    public static void encode(ByteBuffer buf, KvNode n) {
        if (n == null) {
            return;
        }
        PbUtil.writeBytes(buf, 2, n.data);
        PbUtil.writeFix64(buf, 3, n.createIndex);
        PbUtil.writeFix64(buf, 4, n.createTime);
        PbUtil.writeFix64(buf, 5, n.updateIndex);
        PbUtil.writeFix64(buf, 6, n.updateTime);
    }

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
                case 3:
                    createIndex = value;
                    break;
                case 4:
                    createTime = value;
                    break;
                case 5:
                    updateIndex = value;
                    break;
                case 6:
                    updateTime = value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == 2) {
                data = parseBytes(buf, fieldLen, currentPos);
            }
            return true;
        }

        @Override
        protected KvNode getResult() {
            return new KvNode(createIndex, createTime, updateIndex, updateTime, data);
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

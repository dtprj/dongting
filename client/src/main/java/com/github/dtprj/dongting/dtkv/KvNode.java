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
import com.github.dtprj.dongting.net.SmallNoCopyWritePacket;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class KvNode {
    protected final boolean dir;

    protected final long createIndex;
    protected final long createTime;
    protected final long updateIndex;
    protected final long updateTime;

    protected final byte[] data;

    public KvNode(long createIndex, long createTime, long updateIndex, long updateTime, boolean dir, byte[] data) {
        this.createIndex = createIndex;
        this.createTime = createTime;
        this.updateIndex = updateIndex;
        this.updateTime = updateTime;
        this.data = data;
        this.dir = dir;
    }

    public static class WritePacket extends SmallNoCopyWritePacket {

        private final KvNode n;

        public WritePacket(KvNode n) {
            this.n = n;
        }

        @Override
        protected int calcActualBodySize() {
            return n == null ? 0 : PbUtil.accurateUnsignedIntSize(1, n.dir ? 1 : 0)
                    + PbUtil.accurateFix64Size(2, n.createIndex)
                    + PbUtil.accurateFix64Size(3, n.createTime)
                    + PbUtil.accurateFix64Size(4, n.updateIndex)
                    + PbUtil.accurateFix64Size(5, n.updateTime)
                    + PbUtil.accurateLengthDelimitedSize(6, n.data == null ? 0 : n.data.length);
        }

        @Override
        protected void encodeBody(ByteBuffer buf) {
            if (n == null) {
                return;
            }
            PbUtil.writeUnsignedInt32(buf, 1, n.dir ? 1 : 0);
            PbUtil.writeFix64(buf, 2, n.createIndex);
            PbUtil.writeFix64(buf, 3, n.createTime);
            PbUtil.writeFix64(buf, 4, n.updateIndex);
            PbUtil.writeFix64(buf, 5, n.updateTime);
            PbUtil.writeBytes(buf, 6, n.data);
        }
    }

    public static class Callback extends PbCallback<KvNode> {

        private boolean dir;
        private long createIndex;
        private long createTime;
        private long updateIndex;
        private long updateTime;
        private byte[] data;

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == 1) {
                dir = value == 1;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            switch (index) {
                case 2:
                    createIndex = value;
                    break;
                case 3:
                    createTime = value;
                    break;
                case 4:
                    updateIndex = value;
                    break;
                case 5:
                    updateTime = value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == 6) {
                data = parseBytes(buf, fieldLen, currentPos);
            }
            return true;
        }

        @Override
        protected KvNode getResult() {
            return new KvNode(createIndex, createTime, updateIndex, updateTime, dir, data);
        }
    }

    public byte[] getData() {
        return data;
    }

    public long getCreateIndex() {
        return createIndex;
    }

    public boolean isDir() {
        return dir;
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

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
public class KvResult {

    private int code;
    private KvNode data;

    public static final KvResult SUCCESS = new KvResult(KvCodes.CODE_SUCCESS);
    public static final KvResult NOT_FOUND = new KvResult(KvCodes.CODE_NOT_FOUND);
    public static final KvResult SUCCESS_OVERWRITE = new KvResult(KvCodes.CODE_SUCCESS_OVERWRITE);

    public KvResult(int code) {
        this.code = code;
    }

    public static int calcActualSize(KvResult r) {
        return PbUtil.accurateUnsignedIntSize(1, r.code) +
                (r.data == null ? 0 : PbUtil.accurateLengthDelimitedSize(2, KvNode.calcActualSize(r.data)));
    }

    public static void encode(ByteBuffer buf, KvResult r) {
        PbUtil.writeUnsignedInt32(buf, 1, r.code);
        if (r.data != null) {
            PbUtil.writeLengthDelimitedPrefix(buf, 2, KvNode.calcActualSize(r.data));
            KvNode.encode(buf, r.data);
        }
    }

    public static class Callback extends PbCallback<KvResult> {
        private KvResult r;
        private KvNode.Callback nodeCallback = new KvNode.Callback();

        @Override
        protected void begin(int len) {
            r = new KvResult(KvCodes.CODE_SUCCESS);
        }

        @Override
        protected boolean end(boolean success) {
            r = null;
            return success;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == 1) {
                r.code = (int) value;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == 2) {
                r.data = parseNested(buf, fieldLen, currentPos, nodeCallback);
            }
            return true;
        }

        @Override
        protected KvResult getResult() {
            return r;
        }
    }

    public int getCode() {
        return code;
    }

    public KvNode getData() {
        return data;
    }

    public void setData(KvNode data) {
        this.data = data;
    }
}

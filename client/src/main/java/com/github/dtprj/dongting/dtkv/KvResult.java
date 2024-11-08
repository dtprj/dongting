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
import com.github.dtprj.dongting.common.ByteArray;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class KvResult implements Encodable {
    private static final int IDX_BIZ_CODE = 1;
    private static final int IDX_NODE = 2;
    private static final int IDX_KEY_IN_DIR = 3;

    private final int bizCode;
    private final KvNode data;
    private final ByteArray keyInDir;
    private final int size;
    private final int sizeOfField1;

    public static final KvResult SUCCESS = new KvResult(KvCodes.CODE_SUCCESS, null);
    public static final KvResult NOT_FOUND = new KvResult(KvCodes.CODE_NOT_FOUND, null);
    public static final KvResult SUCCESS_OVERWRITE = new KvResult(KvCodes.CODE_SUCCESS_OVERWRITE, null);

    public KvResult(int bizCode) {
        this(bizCode, null, null);
    }

    public KvResult(int bizCode, KvNode data) {
        this(bizCode, data, null);
    }

    public KvResult(int bizCode, KvNode data, ByteArray keyInDir) {
        this.bizCode = bizCode;
        this.data = data;
        this.keyInDir = keyInDir;

        this.sizeOfField1 = PbUtil.accurateUnsignedIntSize(IDX_BIZ_CODE, bizCode);
        this.size = sizeOfField1 +
                (data == null ? 0 : PbUtil.accurateLengthDelimitedSize(IDX_NODE, data.actualSize()))
                + (keyInDir == null ? 0 : keyInDir.actualSize());
    }

    @Override
    public int actualSize() {
        return size;
    }

    @Override
    public boolean encode(EncodeContext c, ByteBuffer destBuffer) {
        if (c.stage < IDX_BIZ_CODE) {
            if (destBuffer.remaining() < sizeOfField1) {
                return false;
            }
            PbUtil.writeUnsignedInt32(destBuffer, IDX_BIZ_CODE, bizCode);
            c.stage = IDX_BIZ_CODE;
        }
        if (c.stage == IDX_BIZ_CODE) {
            if (EncodeUtil.encode(c, destBuffer, IDX_NODE, data)) {
                c.stage = IDX_KEY_IN_DIR;
                return true;
            } else {
                return false;
            }
        }
        if (c.stage == IDX_KEY_IN_DIR) {
            if (EncodeUtil.encode(c, destBuffer, IDX_KEY_IN_DIR, keyInDir)) {
                c.stage = EncodeContext.STAGE_END;
                return true;
            } else {
                return false;
            }
        }
        throw new CodecException(c);
    }

    public static class Callback extends PbCallback<KvResult> {
        // re-used
        private final KvNode.Callback nodeCallback = new KvNode.Callback();
        private int bizCode;
        private KvNode data;
        private ByteArray keyInDir;

        @Override
        protected boolean end(boolean success) {
            bizCode = 0;
            data = null;
            keyInDir = null;
            return success;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == IDX_BIZ_CODE) {
                bizCode = (int) value;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == IDX_NODE) {
                data = parseNested(buf, fieldLen, currentPos, nodeCallback);
            } else if (index == IDX_KEY_IN_DIR) {
                keyInDir = parseByteArray(buf, fieldLen, currentPos);
            }
            return true;
        }

        @Override
        protected KvResult getResult() {
            return new KvResult(bizCode, data, keyInDir);
        }
    }

    public int getBizCode() {
        return bizCode;
    }

    public KvNode getData() {
        return data;
    }

}

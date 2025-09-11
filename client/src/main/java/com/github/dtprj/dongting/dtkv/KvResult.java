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
    private final KvNode node;
    private final ByteArray keyInDir;

    private final int encodeSize;

    public static final KvResult SUCCESS = new KvResult(KvCodes.SUCCESS);
    public static final KvResult NOT_FOUND = new KvResult(KvCodes.NOT_FOUND);
    public static final KvResult SUCCESS_OVERWRITE = new KvResult(KvCodes.SUCCESS_OVERWRITE);

    public KvResult(int bizCode) {
        this(bizCode, null, null);
    }

    public KvResult(int bizCode, KvNode node, ByteArray keyInDir) {
        this.bizCode = bizCode;
        this.node = node;
        this.keyInDir = keyInDir;

        this.encodeSize = PbUtil.sizeOfInt32Field(IDX_BIZ_CODE, bizCode)
                + EncodeUtil.sizeOf(IDX_NODE, node)
                + EncodeUtil.sizeOf(IDX_KEY_IN_DIR, keyInDir);
    }

    @Override
    public int actualSize() {
        return encodeSize;
    }

    @Override
    public boolean encode(EncodeContext c, ByteBuffer destBuffer) {
        switch (c.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeInt32(c, destBuffer, IDX_BIZ_CODE, bizCode)) {
                    return false;
                }
                // fall through
            case IDX_BIZ_CODE:
                if (!EncodeUtil.encode(c, destBuffer, IDX_NODE, node)) {
                    return false;
                }
                // fall through
            case IDX_NODE:
                return EncodeUtil.encode(c, destBuffer, IDX_KEY_IN_DIR, keyInDir);
            default:
                throw new CodecException(c);
        }
    }

    // re-used
    public static class Callback extends PbCallback<KvResult> {
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

    public KvNode getNode() {
        return node;
    }

    public ByteArray getKeyInDir() {
        return keyInDir;
    }
}

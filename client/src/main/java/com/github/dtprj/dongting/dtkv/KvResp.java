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
import java.util.ArrayList;
import java.util.List;

/**
 * @author huangli
 */
public class KvResp implements Encodable {
    private static final int IDX_SIZE = 1;
    private static final int IDX_RESULTS = 2;

    private final List<KvResult> results;
    private final int listCount;
    private int size;

    public KvResp(List<KvResult> results) {
        this.results = results;
        this.listCount = results == null ? 0 : results.size();
    }

    @Override
    public int actualSize() {
        if (size == 0) {
            this.size = PbUtil.accurateUnsignedIntSize(IDX_SIZE, listCount)
                    + EncodeUtil.actualSizeOfObjs(IDX_RESULTS, results);
        }
        return size;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        if (context.stage == EncodeContext.STAGE_BEGIN) {
            if (destBuffer.remaining() >= PbUtil.maxUnsignedIntSize()) {
                PbUtil.writeUnsignedInt32(destBuffer, 1, listCount);
                context.stage = IDX_SIZE;
            } else {
                return false;
            }
        }
        if (context.stage == IDX_SIZE) {
            return EncodeUtil.encodeObjs(context, destBuffer, IDX_RESULTS, results);
        }
        throw new CodecException(context);
    }

    // re-used
    public static class Callback extends PbCallback<KvResp> {
        private final KvResult.Callback resultCallback = new KvResult.Callback();

        private int size;
        private ArrayList<KvResult> results;

        @Override
        protected boolean end(boolean success) {
            results = null;
            size = 0;
            return success;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == IDX_SIZE) {
                size = (int) value;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == IDX_RESULTS) {
                if (results == null) {
                    results = size == 0 ? new ArrayList<>() : new ArrayList<>(size);
                }
                KvResult r = parseNested(buf, fieldLen, currentPos, resultCallback);
                if (r != null) {
                    results.add(r);
                }
            }
            return true;
        }

        @Override
        protected KvResp getResult() {
            return new KvResp(results);
        }
    }

    public List<KvResult> getResults() {
        return results;
    }

}

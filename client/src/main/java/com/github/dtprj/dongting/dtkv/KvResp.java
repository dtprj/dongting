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
    private static final int IDX_RESULTS_SIZE = 1;
    private static final int IDX_RESULTS = 2;
    private static final int IDX_CODES_SIZE = 3;
    private static final int IDX_CODES = 4;

    public final List<KvResult> results;
    public final int[] codes;

    private int encodeSize;

    public KvResp(List<KvResult> results, int[] codes) {
        this.results = results;
        this.codes = codes;
    }

    public KvResp(List<KvResult> results) {
        this.results = results;
        this.codes = null;
    }

    public KvResp(int[] codes) {
        this.results = null;
        this.codes = codes;
    }

    @Override
    public int actualSize() {
        if (encodeSize == 0) {
            this.encodeSize = PbUtil.accurateUnsignedIntSize(IDX_RESULTS_SIZE, results == null ? 0 : results.size())
                    + EncodeUtil.actualSizeOfObjs(IDX_RESULTS, results)
                    + PbUtil.accurateUnsignedIntSize(IDX_CODES_SIZE, codes == null ? 0 : codes.length)
                    + PbUtil.accurateFix32Size(IDX_CODES, codes);
        }
        return encodeSize;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeUint32(context, destBuffer, IDX_RESULTS_SIZE, results == null ? 0 : results.size())) {
                    return false;
                }
                // fall through
            case IDX_RESULTS_SIZE:
                if (!EncodeUtil.encodeObjs(context, destBuffer, IDX_RESULTS, results)) {
                    return false;
                }
                // fall through
            case IDX_RESULTS:
                if (!EncodeUtil.encodeUint32(context, destBuffer, IDX_CODES_SIZE, codes == null ? 0 : codes.length)) {
                    return false;
                }
                // fall through
            case IDX_CODES_SIZE:
                return EncodeUtil.encodeFix32s(context, destBuffer, IDX_CODES, codes);
            default:
                throw new CodecException(context);

        }
    }

    public static class Callback extends PbCallback<KvResp> {
        private final KvResult.Callback resultCallback = new KvResult.Callback();

        private int resultsSize;
        private ArrayList<KvResult> results;
        private int codesSize;
        private int[] codes;
        private int codesIdx;

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == IDX_RESULTS_SIZE) {
                resultsSize = (int) value;
            } else if (index == IDX_CODES_SIZE) {
                codesSize = (int) value;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == IDX_RESULTS) {
                if (results == null) {
                    results = resultsSize == 0 ? new ArrayList<>() : new ArrayList<>(resultsSize);
                }
                KvResult r = parseNested(buf, fieldLen, currentPos, resultCallback);
                if (r != null) {
                    results.add(r);
                }
            }
            return true;
        }

        public boolean readFix32(int index, int value) {
            if (index == IDX_CODES) {
                if (codes == null) {
                    codes = new int[codesSize];
                }
                codes[codesIdx] = value;
                codesIdx++;
            }
            return true;
        }

        @Override
        protected KvResp getResult() {
            return new KvResp(results, codes);
        }
    }

}

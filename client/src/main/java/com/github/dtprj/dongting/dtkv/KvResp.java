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
    private static final int IDX_RAFT_INDEX = 1;
    private static final int IDX_RESULTS_SIZE = 2;
    private static final int IDX_RESULTS = 3;

    public final long raftIndex;
    public final List<KvResult> results;

    private int encodeSize;

    public KvResp(long raftIndex, List<KvResult> results) {
        this.raftIndex = raftIndex;
        this.results = results;
    }

    @Override
    public int actualSize() {
        if (encodeSize == 0) {
            this.encodeSize = PbUtil.sizeOfFix64Field(IDX_RAFT_INDEX, raftIndex)
                    + PbUtil.sizeOfInt32Field(IDX_RESULTS_SIZE, results == null ? 0 : results.size())
                    + EncodeUtil.sizeOfEncodableListField(IDX_RESULTS, results);
        }
        return encodeSize;
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
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_RESULTS_SIZE, results == null ? 0 : results.size())) {
                    return false;
                }
                // fall through
            case IDX_RESULTS_SIZE:
                return EncodeUtil.encodeList(context, destBuffer, IDX_RESULTS, results);
            default:
                throw new CodecException(context);

        }
    }

    public static class Callback extends PbCallback<KvResp> {
        private final KvResult.Callback resultCallback = new KvResult.Callback();

        private long raftIndex;
        private int resultsSize;
        private ArrayList<KvResult> results;

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == IDX_RESULTS_SIZE) {
                resultsSize = (int) value;
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

        @Override
        public boolean readFix64(int index, long value) {
            if (index == IDX_RAFT_INDEX) {
                this.raftIndex = value;
            }
            return true;
        }

        @Override
        protected KvResp getResult() {
            return new KvResp(raftIndex, results);
        }
    }

}

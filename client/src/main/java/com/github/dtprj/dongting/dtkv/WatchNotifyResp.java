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

import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbUtil;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class WatchNotifyResp implements Encodable {
    private static final int IDX_RESULTS_SIZE = 1;
    private static final int IDX_RESULTS = 2;

    private final int[] results;

    public WatchNotifyResp(int[] results) {
        this.results = results;
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt32Field(IDX_RESULTS_SIZE, results == null ? 0 : results.length)
                + PbUtil.sizeOfInt32Field(IDX_RESULTS, results);
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_RESULTS_SIZE, results == null ? 0 : results.length)) {
                    return false;
                }
                // fall through
            case IDX_RESULTS_SIZE:
                return EncodeUtil.encodeInt32s(context, destBuffer, IDX_RESULTS, results);
        }
        throw new IllegalStateException("stage=" + context.stage);
    }
}

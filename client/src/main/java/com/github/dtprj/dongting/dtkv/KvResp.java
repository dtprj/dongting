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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author huangli
 */
public class KvResp implements Encodable {
    private static final int IDX_RESULT = 1;
    private static final int IDX_RESULTS = 2;
    private static final int IDX_CHILDREN = 3;

    private final KvNode result;
    private final List<KvResult> results;
    private final List<KvNode> children;
    private int size;

    public KvResp(KvNode result, List<KvResult> results, List<KvNode> children) {
        this.result = result;
        this.results = results;
        this.children = children;
    }

    @Override
    public int actualSize() {
        if (size == 0) {
            this.size = EncodeUtil.actualSize(IDX_RESULT, result) +
                    EncodeUtil.actualSizeOfObjs(IDX_RESULTS, results) +
                    EncodeUtil.actualSizeOfObjs(IDX_CHILDREN, children);
        }
        return size;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        if (context.stage == EncodeContext.STAGE_BEGIN) {
            if (EncodeUtil.encode(context, destBuffer, IDX_RESULT, result)) {
                context.stage = IDX_RESULT;
            } else {
                return false;
            }
        }
        if (context.stage == IDX_RESULT) {
            if (EncodeUtil.encodeObjs(context, destBuffer, IDX_RESULTS, results)) {
                context.stage = IDX_RESULTS;
            } else {
                return false;
            }
        }
        if (context.stage == IDX_RESULTS) {
            if (EncodeUtil.encodeObjs(context, destBuffer, IDX_CHILDREN, children)) {
                context.stage = IDX_CHILDREN;
                return true;
            } else {
                return false;
            }
        }
        throw new CodecException(context);
    }

    // re-used
    public static class Callback extends PbCallback<KvResp> {
        private final KvNode.Callback nodeCallback = new KvNode.Callback();
        private final KvResult.Callback resultCallback = new KvResult.Callback();

        private KvNode result;
        private List<KvResult> results;
        private List<KvNode> children;

        @Override
        protected boolean end(boolean success) {
            result = null;
            results = null;
            children = null;
            return success;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            switch (index) {
                case IDX_RESULT:
                    result = parseNested(buf, fieldLen, currentPos, nodeCallback);
                    break;
                case IDX_RESULTS:
                    if (results == null) {
                        results = new ArrayList<>();
                    }
                    KvResult r = parseNested(buf, fieldLen, currentPos, resultCallback);
                    if(r != null) {
                        results.add(r);
                    }
                    break;
                case IDX_CHILDREN:
                    if (children == null) {
                        children = new ArrayList<>();
                    }
                    KvNode c = parseNested(buf, fieldLen, currentPos, nodeCallback);
                    if(c != null) {
                        children.add(c);
                    }
                    break;
            }
            return true;
        }

        @Override
        protected KvResp getResult() {
            return new KvResp(result, results, children);
        }
    }

    public KvNode getResult() {
        return result;
    }

    public List<KvResult> getResults() {
        return results;
    }

    public List<KvNode> getChildren() {
        return children;
    }
}

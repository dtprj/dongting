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

import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.SmallNoCopyWritePacket;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author huangli
 */
public class KvResp {
    private KvNode result;
    private List<KvResult> results;
    private List<KvNode> children;

    private class WritePacket extends SmallNoCopyWritePacket {
        @Override
        protected int calcActualBodySize() {
            int x = 0;
            if (result != null) {
                x += PbUtil.accurateLengthDelimitedSize(1, KvNode.calcActualSize(result));
            }
            if (results != null) {
                for (int s = results.size(), i = 0; i < s; i++) {
                    x += PbUtil.accurateLengthDelimitedSize(2, KvResult.calcActualSize(results.get(i)));
                }
            }
            if (children != null) {
                for (KvNode n : children) {
                    x += PbUtil.accurateLengthDelimitedSize(3, KvNode.calcActualSize(n));
                }
            }
            return x;
        }

        @Override
        protected void encodeBody(ByteBuffer buf) {
            if (result != null) {
                x += PbUtil.accurateLengthDelimitedSize(1, KvNode.calcActualSize(result));
            }
            if (results != null) {
                for (int s = results.size(), i = 0; i < s; i++) {
                    x += PbUtil.accurateLengthDelimitedSize(2, KvResult.calcActualSize(results.get(i)));
                }
            }
            if (children != null) {
                for (KvNode n : children) {
                    x += PbUtil.accurateLengthDelimitedSize(3, KvNode.calcActualSize(n));
                }
            }
        }
    }


    public KvNode getResult() {
        return result;
    }

    public void setResult(KvNode result) {
        this.result = result;
    }

    public List<KvResult> getResults() {
        return results;
    }

    public void setResults(List<KvResult> results) {
        this.results = results;
    }

    public List<KvNode> getChildren() {
        return children;
    }

    public void setChildren(List<KvNode> children) {
        this.children = children;
    }
}

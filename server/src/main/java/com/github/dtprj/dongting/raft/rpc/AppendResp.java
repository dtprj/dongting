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
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.codec.PbCallback;

/**
 * @author huangli
 */
//  uint32 term = 1;
//  uint32 success = 2;
//  ///////////////////////////
//  uint32 append_code = 3;
//  uint32 suggest_term = 4;
//  fixed64 suggest_index = 5;
public class AppendResp {
    private int term;
    private boolean success;
    private int appendCode;
    private int suggestTerm;
    private long suggestIndex;

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getAppendCode() {
        return appendCode;
    }

    public int getSuggestTerm() {
        return suggestTerm;
    }

    public long getSuggestIndex() {
        return suggestIndex;
    }

    // re-used
    public static class Callback extends PbCallback<AppendResp> {

        private AppendResp result;

        @Override
        protected void begin(int len) {
            result = new AppendResp();
        }

        @Override
        protected boolean end(boolean success) {
            result = null;
            return success;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    result.term = (int) value;
                    break;
                case 2:
                    result.success = value != 0;
                    break;
                case 3:
                    result.appendCode = (int) value;
                    break;
                case 4:
                    result.suggestTerm = (int) value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readFix64(int index, long value) {
            if (index == 5) {
                result.suggestIndex = value;
            }
            return true;
        }

        @Override
        public AppendResp getResult() {
            return result;
        }
    }
}

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

import com.github.dtprj.dongting.pb.PbCallback;

/**
 * @author huangli
 */
//  uint32 term = 1;
//  uint32 success = 2;
public class AppendRespCallback extends PbCallback {
    private int term;
    private boolean success;
    private int appendCode;
    private int reqTerm;
    private int maxLogTerm;
    private long maxLogIndex;

    @Override
    public boolean readVarNumber(int index, long value) {
        switch (index) {
            case 1:
                term = (int) value;
                break;
            case 2:
                success = value != 0;
                break;
            case 3:
                appendCode = (int) value;
                break;
            case 4:
                reqTerm = (int) value;
                break;
            case 5:
                maxLogTerm = (int) value;
                break;
        }
        return true;
    }

    @Override
    public boolean readFix64(int index, long value) {
        switch (index) {
            case 6:
                maxLogIndex = value;
                break;
        }
        return true;
    }

    @Override
    public Object getResult() {
        return this;
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getAppendCode() {
        return appendCode;
    }

    public int getReqTerm() {
        return reqTerm;
    }

    public int getMaxLogTerm() {
        return maxLogTerm;
    }

    public long getMaxLogIndex() {
        return maxLogIndex;
    }
}

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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.raft.rpc.AppendReq;
import com.github.dtprj.dongting.raft.rpc.AppendResp;
import com.github.dtprj.dongting.raft.sm.RaftCodecFactory;

import java.util.function.Function;

/**
 * @author huangli
 */
public final class DecodeContextEx extends DecodeContext {

    private AppendReq.Callback appendReqCallback;
    private AppendResp.Callback appendRespCallback;

    public DecodeContextEx() {
    }

    @Override
    protected DecodeContext createNestedInstance() {
        return new DecodeContextEx();
    }

    public AppendReq.Callback createOrGetAppendReqCallback(Function<Integer, RaftCodecFactory> decoderFactory) {
        if (appendReqCallback == null) {
            appendReqCallback = new AppendReq.Callback(decoderFactory);
        }
        return appendReqCallback;
    }

    public AppendResp.Callback createOrGetAppendRespCallback() {
        if (appendRespCallback == null) {
            appendRespCallback = new AppendResp.Callback();
        }
        return appendRespCallback;
    }
}

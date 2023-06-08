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

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.ByteArrayDecoder;
import com.github.dtprj.dongting.codec.ByteArrayEncoder;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.codec.StrDecoder;
import com.github.dtprj.dongting.codec.StrEncoder;
import com.github.dtprj.dongting.raft.server.RaftInput;
import com.github.dtprj.dongting.raft.sm.Snapshot;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
public class DtKV implements StateMachine {
    public static final int BIZ_TYPE_GET = 0;
    public static final int BIZ_TYPE_PUT = 1;
    public static final int BIZ_TYPE_REMOVE = 2;
    // public static final int BIZ_TYPE_LIST = 3;
    // public static final int BIZ_TYPE_CAS = 4;

    private final DtKvImpl impl = new DtKvImpl();

    @Override
    public Decoder<?> createDecoder(int bizType, boolean header) {
        switch (bizType) {
            case BIZ_TYPE_GET:
            case BIZ_TYPE_REMOVE:
                return header ? StrDecoder.INSTANCE : null;
            case BIZ_TYPE_PUT:
                return header ? StrDecoder.INSTANCE : ByteArrayDecoder.INSTANCE;
            default:
                throw new IllegalArgumentException("unknown bizType " + bizType);
        }
    }

    @Override
    public Encoder<?> createEncoder(int bizType, boolean header) {
        switch (bizType) {
            case BIZ_TYPE_GET:
            case BIZ_TYPE_REMOVE:
                return header ? new StrEncoder() : null;
            case BIZ_TYPE_PUT:
                return header ? new StrEncoder() : ByteArrayEncoder.INSTANCE;
            default:
                throw new IllegalArgumentException("unknown bizType " + bizType);
        }
    }

    @Override
    public CompletableFuture<Object> exec(long index, RaftInput input) {
        String key = (String) input.getHeader();
        switch (input.getBizType()) {
            case BIZ_TYPE_GET:
                return impl.get(key);
            case BIZ_TYPE_PUT:
                return impl.put(index, key, (byte[]) input.getBody());
            case BIZ_TYPE_REMOVE:
                return impl.remove(key);
            default:
                throw new IllegalArgumentException("unknown bizType " + input.getBizType());
        }
    }

    @Override
    public void installSnapshot(long lastIncludeIndex, int lastIncludeTerm, long offset, boolean done, RefBuffer data) {

    }

    @Override
    public Snapshot takeSnapshot() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}

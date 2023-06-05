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
package com.github.dtprj.dongting.raft.sm;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.raft.server.RaftInput;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public interface StateMachine<H, B, O> extends AutoCloseable {

    Supplier<Decoder<H>> getHeaderDecoder();

    Supplier<Encoder<H>> getHeaderEncoder();

    Supplier<Decoder<B>> getBodyDecoder();

    Supplier<Encoder<B>> getBodyEncoder();

    CompletableFuture<O> exec(long index, RaftInput<H, B> input);

    void installSnapshot(long lastIncludeIndex, int lastIncludeTerm, long offset, boolean done, RefBuffer data);

    Snapshot takeSnapshot();

}

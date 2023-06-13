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

import com.github.dtprj.dongting.codec.Decoder;
import com.github.dtprj.dongting.codec.Encoder;

/**
 * @author huangli
 */
public interface RaftCodecFactory {

    /**
     * this method is called in raft thread or io thread.
     */
    Decoder<?> createDecoder(int bizType, boolean header);

    /**
     * this method is called in raft thread or io thread.
     */
    Encoder<?> createEncoder(int bizType, boolean header);
}

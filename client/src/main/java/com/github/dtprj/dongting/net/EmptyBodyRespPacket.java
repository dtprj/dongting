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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.codec.EncodeContext;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class EmptyBodyRespPacket extends RetryableWritePacket {

    public EmptyBodyRespPacket(int respCode) {
        setRespCode(respCode);
    }

    @Override
    protected int calcActualBodySize() {
        return 0;
    }

    @Override
    protected boolean encodeBody(EncodeContext context, ByteBuffer dest) {
        return true;
    }
}

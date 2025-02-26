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
import com.github.dtprj.dongting.codec.EncodeUtil;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class PbBytesWritePacket extends RetryableWritePacket {

    private final byte[] bs;

    public PbBytesWritePacket(int command, byte[] bs) {
        this.command = command;
        this.bs = bs;
    }

    @Override
    protected int calcActualBodySize() {
        return bs == null ? 0 : bs.length;
    }

    @Override
    protected boolean encodeBody(EncodeContext c, ByteBuffer dest) {
        return EncodeUtil.encodeBytes(c, dest, 1, bs);
    }
}

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
package com.github.dtprj.dongting.codec;

import com.github.dtprj.dongting.net.NetException;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class BoolDecoder extends Decoder<Boolean> {

    public static final BoolDecoder INSTANCE = new BoolDecoder();

    private BoolDecoder() {
    }

    @Override
    public Boolean decode(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos) {
        if (bodyLen != 1 || currentPos != 0) {
            throw new NetException("invalid bool data");
        }
        if (buffer.hasRemaining()) {
            byte b = buffer.get();
            if (b == -1) {
                return null;
            } else if (b == 0) {
                return false;
            } else if (b == 1) {
                return true;
            } else {
                throw new NetException("invalid bool data:" + b);
            }
        } else {
            return null;
        }
    }

}

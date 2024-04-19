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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public class StrEncoder extends ByteArrayEncoder {

    private final String str;

    public StrEncoder(String str) {
        super(str == null ? null : str.getBytes(StandardCharsets.UTF_8));
        this.str = str;
    }

    public String getStr() {
        return str;
    }

    @Override
    public String toString() {
        return str;
    }

    public static final Decoder<StrEncoder> DECODER = new Decoder<StrEncoder>() {
        @Override
        public StrEncoder doDecode(DecodeContext context, ByteBuffer buffer, int bodyLen, int currentPos) {
            String s = StrFiledDecoder.decode0(context, buffer, bodyLen, currentPos);
            if (s != null) {
                return new StrEncoder(s);
            } else {
                return null;
            }
        }

        @Override
        public void finish(DecodeContext context) {
            StrFiledDecoder.finish0(context);
        }
    };
}

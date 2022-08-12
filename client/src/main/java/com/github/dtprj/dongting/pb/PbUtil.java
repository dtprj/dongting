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
package com.github.dtprj.dongting.pb;

/**
 * encode/decode util for proto buffer 3.
 * <p>
 * see: https://developers.google.com/protocol-buffers/docs/encoding
 */
public class PbUtil {

    // max support 4 bytes (int) to represent a field index, so there is 4 * 7 (base 128) - 3(type) = 25 valid bits.
    // max value is 33554431
    public static final int MAX_SUPPORT_FIELD_INDEX = (1 << 25) - 1;

    public static final int TYPE_VAR_INT = 0;
    public static final int TYPE_FIX64 = 1;
    public static final int TYPE_LENGTH_DELIMITED = 2;
    public static final int TYPE_START_GROUP = 3;
    public static final int TYPE_END_GROUP = 4;
    public static final int TYPE_FIX32 = 5;

    public static int toTag(int type, int index) {
        if (index > MAX_SUPPORT_FIELD_INDEX || index <= 0) {
            throw new IllegalArgumentException(String.valueOf(index));
        }
        int value = (index << 3) | type;

        int encode = 0;
        for (int i = 0; i < 4; i++) {
            encode |= (value & 0x7F);
            value >>>= 7;
            if (value == 0) {
                break;
            }
            encode |= 0x80;
            encode <<= 8;
        }
        return encode;
    }

}

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

import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbParser;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class HandshakeBody {
    long magic1;
    long magic2;
    int majorVersion;
    int minorVersion;
    ConfigBody config;

    public static class Callback extends PbCallback<HandshakeBody> {
        private final HandshakeBody result = new HandshakeBody();

        @Override
        public HandshakeBody getResult() {
            return result;
        }

        @Override
        public boolean readVarNumber(int index, long value) {
            switch (index) {
                case 1:
                    result.magic1 = value;
                    break;
                case 2:
                    result.magic2 = value;
                    break;
                case 3:
                    result.majorVersion = (int) value;
                    break;
                case 4:
                    result.minorVersion = (int) value;
                    break;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == 8) {
                PbParser nestedParser;
                ConfigBody.Callback nestedCallback;
                if (currentPos == 0) {
                    nestedCallback = new ConfigBody.Callback();
                    nestedParser = parser.createOrGetNestedParser(nestedCallback, fieldLen);
                } else {
                    nestedParser = parser.getNestedParser();
                    nestedCallback = (ConfigBody.Callback) nestedParser.getCallback();
                }
                nestedParser.parse(buf);
                if (currentPos == fieldLen) {
                    result.config = nestedCallback.getResult();
                }
            }
            return true;
        }
    }
}

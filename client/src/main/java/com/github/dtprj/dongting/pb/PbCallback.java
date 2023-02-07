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

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public abstract class PbCallback {

    public boolean readVarNumber(int index, long value) {
        throw new UnsupportedOperationException();
    }

    public boolean readFix32(int index, int value) {
        throw new UnsupportedOperationException();
    }

    public boolean readFix64(int index, long value) {
        throw new UnsupportedOperationException();
    }

    public boolean readBytes(int index, ByteBuffer buf, int len, boolean begin, boolean end) {
        throw new UnsupportedOperationException();
    }

    public void begin(int len, PbParser parser) {
    }

    public void end(boolean success) {
    }

    public Object getResult() {
        throw new UnsupportedOperationException();
    }
}

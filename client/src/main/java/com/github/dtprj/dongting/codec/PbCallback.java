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

/**
 * @author huangli
 */
public abstract class PbCallback<T> {

    // TODO remove this?
    protected PbParser parser;

    public boolean readVarNumber(int index, long value) {
        return true;
    }

    public boolean readFix32(int index, int value) {
        return true;
    }

    public boolean readFix64(int index, long value) {
        return true;
    }

    public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
        return true;
    }

    public void begin(int len, PbParser parser) {
        this.parser = parser;
    }

    public void end(boolean success) {
        this.parser = null;
    }

    public T getResult() {
        throw new UnsupportedOperationException();
    }

    public void clean() {
    }
}

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
package com.github.dtprj.dongting.remoting;

import com.github.dtprj.dongting.pb.PbCallback;

import java.nio.ByteBuffer;

// TODO not finished
class IoCallback implements PbCallback {
    @Override
    public void begin() {

    }

    @Override
    public void end() {

    }

    @Override
    public void readInt(int index, int value) {
        System.out.println("read " + index + ":" + value);
    }

    @Override
    public void readLong(int index, long value) {
        System.out.println("read " + index + ":" + value);
    }

    @Override
    public void readBytes(int index, ByteBuffer buf) {
        byte[] bs = new byte[buf.remaining()];
        buf.get(bs);
        System.out.println("read " + index + ":" + new String(bs));
    }
}

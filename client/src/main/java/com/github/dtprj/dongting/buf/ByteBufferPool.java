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
package com.github.dtprj.dongting.buf;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public abstract class ByteBufferPool {

    protected final boolean direct;

    public ByteBufferPool(boolean direct) {
        this.direct = direct;
    }

    public boolean isDirect() {
        return direct;
    }

    public abstract ByteBuffer borrow(int requestSize);

    public abstract void release(ByteBuffer buf);

    abstract ByteBuffer allocate(int requestSize);

    public abstract void clean();

    // TODO return a stat object, not a string
    public abstract String formatStat();
}

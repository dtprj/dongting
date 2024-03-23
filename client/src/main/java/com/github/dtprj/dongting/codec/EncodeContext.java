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

import com.github.dtprj.dongting.buf.RefBufferFactory;

/**
 * @author huangli
 */
public class EncodeContext {
    private final RefBufferFactory heapPool;
    private Object status;

    public EncodeContext(RefBufferFactory heapPool) {
        this.heapPool = heapPool;
    }

    public RefBufferFactory getHeapPool() {
        return heapPool;
    }

    public Object getStatus() {
        return status;
    }

    public void setStatus(Object status) {
        this.status = status;
    }

    public void reset() {
        this.status = null;
    }
}

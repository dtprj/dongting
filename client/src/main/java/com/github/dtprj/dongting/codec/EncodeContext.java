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

    public static final int STAGE_BEGIN = 0;
    public static final int STAGE_END = Integer.MAX_VALUE;

    public Object status;
    public int stage;
    public int pending;

    private EncodeContext nested;
    private boolean nestedUse;

    public EncodeContext(RefBufferFactory heapPool) {
        this.heapPool = heapPool;
    }

    public void reset() {
        this.status = null;
        this.stage = STAGE_BEGIN;
        this.pending = 0;
        if (nestedUse) {
            nested.reset();
            nestedUse = false;
        }
    }

    public EncodeContext createOrGetNestedContext(boolean reset) {
        if (nested == null) {
            nested = new EncodeContext(heapPool);
        }
        if (!nestedUse) {
            nestedUse = true;
        }
        if (reset) {
            nested.reset();
        }
        return nested;
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

    public int getStage() {
        return stage;
    }

    public void setStage(int stage) {
        this.stage = stage;
    }

    public int getPending() {
        return pending;
    }

    public void setPending(int pending) {
        this.pending = pending;
    }
}

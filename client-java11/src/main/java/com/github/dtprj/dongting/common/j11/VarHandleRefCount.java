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
package com.github.dtprj.dongting.common.j11;

import com.github.dtprj.dongting.common.AbstractRefCount;
import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.Processor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * @author huangli
 */
public class VarHandleRefCount extends AbstractRefCount {
    private static final VarHandle REF_CNT;

    static {
        try {
            REF_CNT = MethodHandles.lookup()
                    .findVarHandle(AbstractRefCount.class, "refCnt", int.class);
        } catch (Exception e) {
            throw new DtException(e);
        }
    }


    public VarHandleRefCount() {
        REF_CNT.set(this, 2);
    }

    @Override
    protected int getAndAdd(int rawIncrement) {
        return (int) REF_CNT.getAndAdd(this, rawIncrement);
    }

    @Override
    protected int getPlain() {
        return (int) REF_CNT.get(this);
    }

    @Override
    protected int getVolatile() {
        return (int) REF_CNT.getVolatile(this);
    }

    @Override
    protected void doSpin(int count) {
        if (count <= 10 && Processor.processorCount() > 1) {
            Thread.onSpinWait();
        } else {
            Thread.yield();
        }
    }

    @Override
    protected boolean weakCAS(int expect, int newValue) {
        return REF_CNT.weakCompareAndSetPlain(this, expect, newValue);
    }

}



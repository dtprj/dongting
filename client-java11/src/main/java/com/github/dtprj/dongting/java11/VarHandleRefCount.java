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
package com.github.dtprj.dongting.java11;

import com.github.dtprj.dongting.common.Processor;
import com.github.dtprj.dongting.common.RefCount;
import com.github.dtprj.dongting.common.RefCountUpdater;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * @author huangli
 */
public class VarHandleRefCount extends RefCountUpdater {
    static final VarHandle REF_CNT;

    static {
        try {
            REF_CNT = MethodHandles.privateLookupIn(RefCount.class, MethodHandles.lookup())
                    .findVarHandle(RefCount.class, "refCnt", int.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private static final VarHandleRefCount INSTANCE = new VarHandleRefCount();

    private VarHandleRefCount() {
    }

    public static VarHandleRefCount getInstance() {
        return INSTANCE;
    }

    @Override
    public void init(RefCount instance) {
        REF_CNT.set(instance, 2);
    }

    @Override
    protected int getAndAdd(RefCount instance, int rawIncrement) {
        return (int) REF_CNT.getAndAdd(instance, rawIncrement);
    }

    @Override
    protected int getPlain(RefCount instance) {
        return (int) REF_CNT.get(instance);
    }

    @Override
    protected int getVolatile(RefCount instance) {
        return (int) REF_CNT.getVolatile(instance);
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
    protected boolean weakCAS(RefCount instance, int expect, int newValue) {
        return REF_CNT.weakCompareAndSetPlain(instance, expect, newValue);
    }

}



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
package com.github.dtprj.dongting.java8;

import com.github.dtprj.dongting.common.AbstractRefCount;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author huangli
 */
public class Java8RefCount extends AbstractRefCount {

    private static final AtomicIntegerFieldUpdater<AbstractRefCount> UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractRefCount.class, "refCnt");

    public Java8RefCount() {
        super();
        UPDATER.lazySet(this, 2);
    }

    @Override
    protected int getAndAdd(int rawIncrement) {
        return UPDATER.getAndAdd(this, rawIncrement);
    }

    @Override
    protected int getPlain() {
        // not use Unsafe, so we can't perform plain read
        return UPDATER.get(this);
    }

    @Override
    protected int getVolatile() {
        return UPDATER.get(this);
    }

    @Override
    protected void doSpin(int count) {
        Thread.yield(); // this benefits throughput under high contention
    }

    @Override
    protected boolean weakCAS(int expect, int newValue) {
        return UPDATER.weakCompareAndSet(this, expect, newValue);
    }

}

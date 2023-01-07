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

import com.github.dtprj.dongting.common.RefCount;
import com.github.dtprj.dongting.common.RefCountUpdater;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author huangli
 */
public class Java8RefCountUpdater extends RefCountUpdater {

    private static final AtomicIntegerFieldUpdater<RefCount> UPDATER = RefCount.REF_CNT_UPDATER;
    private static final Java8RefCountUpdater INSTANCE = new Java8RefCountUpdater();

    private Java8RefCountUpdater() {
    }

    public static Java8RefCountUpdater getInstance() {
        return INSTANCE;
    }

    @Override
    public void init(RefCount instance) {
        UPDATER.lazySet(instance, 2);
    }

    @Override
    protected int getAndAdd(RefCount instance, int rawIncrement) {
        return UPDATER.getAndAdd(instance, rawIncrement);
    }

    @Override
    protected int getPlain(RefCount instance) {
        // not use Unsafe, so we can't perform plain read
        return UPDATER.get(instance);
    }

    @Override
    protected int getVolatile(RefCount instance) {
        return UPDATER.get(instance);
    }

    @Override
    protected void doSpin(int count) {
        Thread.yield(); // this benefits throughput under high contention
    }

    @Override
    protected boolean weakCAS(RefCount instance, int expect, int newValue) {
        return UPDATER.weakCompareAndSet(instance, expect, newValue);
    }

}


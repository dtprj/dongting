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
package com.github.dtprj.dongting.fiber;

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.RefBufferFactory;

/**
 * @author huangli
 */
public final class DispatcherThread extends Thread {

    FiberGroup currentGroup;

    private ByteBufferPool directPool;
    private RefBufferFactory heapPool;

    DispatcherThread(Runnable r, String name) {
        super(r, name);
    }

    static DispatcherThread currentDispatcherThread() {
        try {
            return (DispatcherThread) Thread.currentThread();
        } catch (ClassCastException e) {
            throw new FiberException("not run in dispatcher thread");
        }
    }

    static FiberGroup currentGroup() {
        return currentDispatcherThread().currentGroup;
    }

    static Fiber currentFiber() {
        return currentDispatcherThread().currentGroup.currentFiber;
    }

    public ByteBufferPool getDirectPool() {
        return directPool;
    }

    public void setDirectPool(ByteBufferPool directPool) {
        this.directPool = directPool;
    }

    public RefBufferFactory getHeapPool() {
        return heapPool;
    }

    public void setHeapPool(RefBufferFactory heapPool) {
        this.heapPool = heapPool;
    }
}

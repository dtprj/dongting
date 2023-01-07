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
package com.github.dtprj.dongting.common;

/**
 * see netty ReferenceCountUpdater
 * For the updated int field:
 *   Even => "real" refcount is (refCnt >>> 1)
 *   Odd  => "real" refcount is 0
 * (x & y) appears to be surprisingly expensive relative to (x == y). Thus this class uses
 * a fast-path in some places for most common low values when checking for live (even) refcounts,
 * for example: if (rawCnt == 2 || rawCnt == 4 || (rawCnt & 1) == 0) { ...
 *
 * @author huangli
 */
public abstract class RefCountUpdater extends AbstractRefCountUpdater {

    protected RefCountUpdater() {
    }

    public abstract void init(RefCount instance);

    protected abstract int getAndAdd(RefCount instance, int rawIncrement);

    protected abstract int getPlain(RefCount instance);

    protected abstract int getVolatile(RefCount instance);

    protected abstract void doSpin(int count);

    protected abstract boolean weakCAS(RefCount instance, int expect, int newValue);

    private int realRefCnt(int rawCnt) {
        return rawCnt != 2 && rawCnt != 4 && (rawCnt & 1) != 0 ? 0 : rawCnt >>> 1;
    }

    /**
     * Like {@link #realRefCnt(int)} but throws if realCount == 0
     */
    private int toLiveRealRefCnt(int rawCnt) {
        if (rawCnt == 2 || rawCnt == 4 || (rawCnt & 1) == 0) {
            return rawCnt >>> 1;
        }
        // odd rawCnt => already deallocated
        throw new DtException("already released");
    }

    @Override
    public void retain(RefCount instance, int increment) {
        ObjUtil.checkPositive(increment, "increment");
        retain0(instance, increment, increment << 1);
    }

    private void retain0(RefCount instance, int increment, int rawIncrement) {
        int oldRef = getAndAdd(instance, rawIncrement);
        if (oldRef != 2 && oldRef != 4 && (oldRef & 1) != 0) {
            throw new DtException("the count is 0, increment=" + increment);
        }
        if (oldRef <= 0 || oldRef + rawIncrement < oldRef) {
            // overflow case
            getAndAdd(instance, -rawIncrement);
            throw new DtException("retain overflow: " + realRefCnt(oldRef) + "," + increment);
        }
    }

    @Override
    public boolean release(RefCount instance, int decrement) {
        ObjUtil.checkPositive(decrement, "decrement");
        int rawCnt = getPlain(instance);
        int realCnt = toLiveRealRefCnt(rawCnt);
        return decrement == realCnt ? tryFinalRelease0(instance, rawCnt) || retryRelease0(instance, decrement)
                : nonFinalRelease0(instance, decrement, rawCnt, realCnt);
    }

    private boolean tryFinalRelease0(RefCount instance, int expectRawCnt) {
        return weakCAS(instance, expectRawCnt, 1); // any odd number will work
    }

    private boolean nonFinalRelease0(RefCount instance, int decrement, int rawCnt, int realCnt) {
        if (decrement < realCnt
                && weakCAS(instance, rawCnt, rawCnt - (decrement << 1))) {
            return false;
        }
        return retryRelease0(instance, decrement);
    }

    private boolean retryRelease0(RefCount instance, int decrement) {
        int count = 0;
        for (; ; ) {
            int rawCnt = getVolatile(instance), realCnt = toLiveRealRefCnt(rawCnt);
            if (decrement == realCnt) {
                if (tryFinalRelease0(instance, rawCnt)) {
                    return true;
                }
            } else if (decrement < realCnt) {
                // all changes to the raw count are 2x the "real" change
                if (weakCAS(instance, rawCnt, rawCnt - (decrement << 1))) {
                    return false;
                }
            } else {
                throw new DtException("decrement>realCnt," + decrement + "," + realCnt);
            }
            doSpin(++count);
        }
    }
}

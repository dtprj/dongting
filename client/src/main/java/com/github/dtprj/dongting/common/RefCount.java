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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author huangli
 * <p>
 * see netty ReferenceCountUpdater.
 */
public abstract class RefCount<T> {

    private static final DtLog log = DtLogs.getLogger(RefCount.class);

    private static final RefCountFactory FACTORY;

    static {
        if (JavaVersion.javaVersion() <= 8) {
            FACTORY = new Java8RefCountFactory();
        } else {
            RefCountFactory f = null;
            try {
                Class<?> clz = Class.forName("com.github.dtprj.dongting.common.VarHandleRefCountFactory");
                f = (RefCountFactory) clz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                log.error("", e);
            }
            FACTORY = f != null ? f : new Java8RefCountFactory();
        }
    }

    private final T data;

    protected RefCount(T data) {
        this.data = data;
    }

    public static <T> RefCount<T> newInstance(T data) {
        return FACTORY.newInstance(data);
    }

    /**
     * return RefCount instance that is not threadSafe
     */
    public static <T> RefCount<T> newPlainInstance(T data) {
        return new PlainRefCount<>(data);
    }

    public abstract void retain();

    public abstract void retain(int increment);

    public abstract boolean release();

    public abstract boolean release(int decrement);

    public T getData() {
        return data;
    }
}

abstract class AbstractRefCount<T> extends RefCount<T> {

    /*
     * Implementation notes:
     *
     * For the updated int field:
     *   Even => "real" refcount is (refCnt >>> 1)
     *   Odd  => "real" refcount is 0
     *
     * (x & y) appears to be surprisingly expensive relative to (x == y). Thus this class uses
     * a fast-path in some places for most common low values when checking for live (even) refcounts,
     * for example: if (rawCnt == 2 || rawCnt == 4 || (rawCnt & 1) == 0) { ...
     */
    @SuppressWarnings({"unused", "FieldMayBeFinal"})
    protected volatile int refCnt;

    protected AbstractRefCount(T data) {
        super(data);
    }

    protected abstract int getAndAdd(int rawIncrement);

    protected abstract int getPlain();

    protected abstract int getVolatile();

    protected abstract void doSpin(int count);

    protected abstract boolean weakCAS(int expect, int newValue);

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
    public void retain() {
        retain0(1, 2);
    }

    @Override
    public void retain(int increment) {
        ObjUtil.checkPositive(increment, "increment");
        retain0(increment, increment << 1);
    }

    private void retain0(int increment, int rawIncrement) {
        int oldRef = getAndAdd(rawIncrement);
        if (oldRef != 2 && oldRef != 4 && (oldRef & 1) != 0) {
            throw new DtException("the count is 0, increment=" + increment);
        }
        if (oldRef <= 0 || oldRef + rawIncrement < oldRef) {
            // overflow case
            getAndAdd(-rawIncrement);
            throw new DtException("retain overflow: " + realRefCnt(oldRef) + "," + increment);
        }
    }

    @Override
    public boolean release() {
        return release(1);
    }

    @Override
    public boolean release(int decrement) {
        ObjUtil.checkPositive(decrement, "decrement");
        int rawCnt = getPlain();
        int realCnt = toLiveRealRefCnt(rawCnt);
        return decrement == realCnt ? tryFinalRelease0(rawCnt) || retryRelease0(decrement)
                : nonFinalRelease0(decrement, rawCnt, realCnt);
    }

    private boolean tryFinalRelease0(int expectRawCnt) {
        return weakCAS(expectRawCnt, 1); // any odd number will work
    }

    private boolean nonFinalRelease0(int decrement, int rawCnt, int realCnt) {
        if (decrement < realCnt
                // all changes to the raw count are 2x the "real" change - overflow is OK
                && weakCAS(rawCnt, rawCnt - (decrement << 1))) {
            return false;
        }
        return retryRelease0(decrement);
    }

    private boolean retryRelease0(int decrement) {
        int count = 0;
        for (; ; ) {
            int rawCnt = getVolatile(), realCnt = toLiveRealRefCnt(rawCnt);
            if (decrement == realCnt) {
                if (tryFinalRelease0(rawCnt)) {
                    return true;
                }
            } else if (decrement < realCnt) {
                // all changes to the raw count are 2x the "real" change
                if (weakCAS(rawCnt, rawCnt - (decrement << 1))) {
                    return false;
                }
            } else {
                throw new DtException("decrement>realCnt," + decrement + "," + realCnt);
            }
            doSpin(++count);
        }
    }

}

class Java8RefCount<T> extends AbstractRefCount<T> {

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<AbstractRefCount> UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractRefCount.class, "refCnt");

    public Java8RefCount(T data) {
        super(data);
        UPDATER.set(this, 2); //TODO can we use lazySet?
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

abstract class RefCountFactory {
    public abstract <T> RefCount<T> newInstance(T data);
}

class Java8RefCountFactory extends RefCountFactory {

    @Override
    public <T> RefCount<T> newInstance(T data) {
        return new Java8RefCount<>(data);
    }
}
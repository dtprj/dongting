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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author huangli
 * <p>
 * see netty ReferenceCountUpdater.
 */
public class RefCount {

    // init this field first
    public static final AtomicIntegerFieldUpdater<RefCount> REF_CNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(RefCount.class, "refCnt");

    private static final AbstractRefCountUpdater UPDATER = VersionFactory.getInstance().newRefCountUpdater(false);
    private static final AbstractRefCountUpdater PLAIN_UPDATER = VersionFactory.getInstance().newRefCountUpdater(true);
    protected final AbstractRefCountUpdater updater;

    @SuppressWarnings({"unused", "FieldMayBeFinal"})
    protected volatile int refCnt;

    public RefCount() {
        this(false, false);
    }

    /**
     * @param plain if true, the RefCount is not thread safe.
     */
    public RefCount(boolean plain, boolean dummy) {
        this(dummy ? null : plain ? PLAIN_UPDATER : UPDATER);
    }

    protected RefCount(AbstractRefCountUpdater updater) {
        this.updater = updater;
        if (updater != null) {
            updater.init(this);
        }
    }

    public void retain() {
        retain(1);
    }

    public void retain(int increment) {
        AbstractRefCountUpdater u = updater;
        if (u != null) {
            u.retain(this, increment);
        }
    }

    public boolean release() {
        return release(1);
    }

    public boolean release(int decrement) {
        AbstractRefCountUpdater u = updater;
        if (u == null) {
            return false;
        }
        boolean r = u.release(this, decrement);
        if (r) {
            doClean();
        }
        return r;
    }

    protected boolean isReleased() {
        AbstractRefCountUpdater u = updater;
        if (u == null) {
            return false;
        }
        return u.isReleased(this);
    }

    protected void doClean() {
    }
}
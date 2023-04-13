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
    private final AbstractRefCountUpdater updater;

    @SuppressWarnings({"unused", "FieldMayBeFinal"})
    protected volatile int refCnt;

    /**
     * @param plain if true, the RefCount is not thread safe.
     */
    public RefCount(boolean plain) {
        this(plain ? PLAIN_UPDATER : UPDATER);
    }

    protected RefCount(AbstractRefCountUpdater updater) {
        this.updater = updater;
        updater.init(this);
    }

    public void retain() {
        retain(1);
    }

    public void retain(int increment) {
        updater.retain(this, increment);
    }

    public boolean release() {
        return release(1);
    }

    public boolean release(int decrement) {
        return updater.release(this, decrement);
    }

    protected boolean isReleased() {
        return updater.isReleased(this);
    }

}
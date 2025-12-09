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

import com.github.dtprj.dongting.common.DtException;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * @author huangli
 */
public class ShareStatusSource {
    public volatile ShareStatus shareStatus;
    protected static final VarHandle SHARE_STATUS;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            SHARE_STATUS = l.findVarHandle(ShareStatusSource.class, "shareStatus", ShareStatus.class);
        } catch (Exception e) {
            throw new DtException(e);
        }
    }

    protected boolean shouldStop;

    public ShareStatusSource() {
        copy(false);
    }

    protected void copy(boolean volatileMode) {
        ShareStatus ss = new ShareStatus(shouldStop);
        if (volatileMode) {
            shareStatus = ss;
        } else {
            SHARE_STATUS.setRelease(this, ss);
        }
    }

    public ShareStatus getShareStatus(boolean volatileMode) {
        if (volatileMode) {
            return shareStatus;
        } else {
            return (ShareStatus) SHARE_STATUS.getAcquire(this);
        }
    }

    public boolean isShouldStop() {
        return shouldStop;
    }
}

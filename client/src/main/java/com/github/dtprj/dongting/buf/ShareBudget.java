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
package com.github.dtprj.dongting.buf;

import com.github.dtprj.dongting.log.BugLog;

/**
 * @author huangli
 */
public class ShareBudget {

    private final long total;
    private final boolean threadSafe;
    private long used;

    ShareBudget(long total, boolean threadSafe) {
        this.total = total;
        this.threadSafe = threadSafe;
    }

    public boolean borrow(int size) {
        if (threadSafe) {
            synchronized (this) {
                return borrow0(size);
            }
        } else {
            return borrow0(size);
        }
    }

    private boolean borrow0(int size) {
        if (used + size <= total) {
            used += size;
            return true;
        }
        return false;
    }

    public void release(int size) {
        if (threadSafe) {
            synchronized (this) {
                release0(size);
            }
        } else {
            release0(size);
        }
    }

    private void release0(int size) {
        used -= size;
        if (used < 0) {
            BugLog.log("ShareBudget underflow: used={}, size={}", used, size);
            used = 0;
        }
    }
}

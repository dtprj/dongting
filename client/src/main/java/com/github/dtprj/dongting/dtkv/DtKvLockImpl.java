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
package com.github.dtprj.dongting.dtkv;

import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.net.NetException;

/**
 * @author huangli
 */
class DtKvLockImpl implements DtKvLock {

    /**
     * Each tryLock should increase the operationId by 1. When receive Commands.DTKV_LOCK_PUSH, check if
     * operationId is the same as the one in push, if not, ignore the push.
     */
    private long operationId;

    private final LockManager lockManager;

    DtKvLockImpl(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    @Override
    public boolean tryLock(long waitLockTimeoutMillis, long leaseMillis) throws KvException, NetException {
        return false;
    }

    @Override
    public void tryLock(long waitLockTimeoutMillis, long leaseMillis, FutureCallback<Boolean> callback) {

    }

    @Override
    public boolean tryLock(long leaseMillis) throws KvException, NetException {
        return false;
    }

    @Override
    public void tryLock(long leaseMillis, FutureCallback<Boolean> callback) {

    }

    @Override
    public void unlock() throws KvException, NetException {

    }

    @Override
    public void unlock(FutureCallback<Void> callback) {

    }

    @Override
    public boolean isHeldByCurrentClient() {
        return false;
    }

    @Override
    public long getLeaseRestMillis() {
        return 0;
    }

    @Override
    public void setLockExpireListener(Runnable listener) {

    }

    @Override
    public void setAutoRenewLeaseIfOwner(boolean autoRenew) {

    }

    @Override
    public void close() {

    }
}

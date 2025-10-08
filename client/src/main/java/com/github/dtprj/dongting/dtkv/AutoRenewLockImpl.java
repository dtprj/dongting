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

import com.github.dtprj.dongting.common.ByteArray;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author huangli
 */
class AutoRenewLockImpl implements AutoRenewLock {

    private final int groupId;
    private final ByteArray key;
    private final long leaseMillis;
    private final AutoRenewLockListener listener;
    private final DistributedLockImpl lock;

    AutoRenewLockImpl(int groupId, ByteArray key, long leaseMillis, AutoRenewLockListener listener,
                      DistributedLockImpl lock, ScheduledExecutorService executeService) {
        this.groupId = groupId;
        this.key = key;
        this.leaseMillis = leaseMillis;
        this.listener = listener;
        this.lock = lock;
        this.lock.setLockExpireListener(listener::onLost);
    }

    @Override
    public boolean isHeldByCurrentClient() {
        return lock.isHeldByCurrentClient();
    }

    @Override
    public long getLeaseRestMillis() {
        return lock.getLeaseRestMillis();
    }

    @Override
    public void close() {
        lock.close();
    }
}

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

/**
 *
 * @author huangli
 */
public interface AutoRenewalLockListener {

    /**
     * Invoked when the lock is successfully acquired. This callback is running in a lock,
     * don't do blocking operations in the callback.
     */
    void onAcquired();

    /**
     * Invoked when the lock is lost (include expire or the AutoRenewalLock instance closed when it held the lock).
     * This callback is running in a lock, don't do blocking operations in the callback.
     */
    void onLost();
}

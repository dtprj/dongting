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
 * The listener of AutoRenewalLock. AutoRenewalLock ensures:
 * <ul>
 *     <li>The onAcquired()/onLost() callback is always invoked in sequential manner.</li>
 *     <li>The onAcquired() will not be invoked after onAcquired()</li>
 *     <li>The onLost() will not be invoked after onLost()</li>
 *     <li>If AutoRenewalLock closed when it holds the lock, the onLost() callback will be invoked.</li>
 * </ul>
 *
 * @author huangli
 */
public interface AutoRenewalLockListener {

    /**
     * Invoked when the lock is successfully acquired.
     * It will execute in bizExecutor of NioClient by default. It's recommended to do non-blocking operations
     * in the listener, because it may block the next callbacks.
     */
    void onAcquired();

    /**
     * Invoked when the lock is lost (include expire or the AutoRenewalLock instance closed when it held the lock).
     * It will execute in bizExecutor of NioClient by default. It's recommended to do non-blocking operations
     * in the listener, because it may block the next callbacks.
     */
    void onLost();
}

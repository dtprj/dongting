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
 * The auto-renewal lock. If the current client is not holding the lock, the client will try to acquire it
 * in background forever; If the lock is held by the current client, the client will try to renew the lease
 * periodically in background.
 *
 * <p>
 * This interface generally used to implement leader election among multiple clients, the client which holds
 * the lock is the leader.
 *
 * <p>
 * Any exception occurred during the acquire or renew operation will be processed internally,
 * and the user just need to provide an AutoRenewLockListener to get notified when the lock is acquired or lost.
 *
 * @see AutoRenewLockListener
 * @author huangli
 */
public interface AutoRenewalLock {
    /**
     * Detect whether the current client is the owner of the lock. This method returns immediately ant will not
     * throw any exception.
     *
     * <p>
     * NOTICE: return false does not mean the lock is not held by current client, because the network error may
     * cause the remote status is unknown to the client.
     *
     * @return True if and only if the current client is guaranteed to be the owner of the lock
     * AND the lease has not expired. False means either the client is not the owner of the lock,
     * or the ownership status cannot be determined (e.g., due to a network error).
     */
    boolean isHeldByCurrentClient();

    /**
     * Get the remaining lease time in milliseconds, return 0 if the lease has expired or the current client is not
     * the owner of the lock or the ownership status cannot be determined (e.g., due to a network error).
     *
     * @return the remaining lease time in milliseconds
     */
    long getLeaseRestMillis();

    /**
     * Safely close the lock, and remove it from KvClient.
     * After close, call createAutoRenewLock() in KvClient will get a new created instance.
     * This method try to asynchronously release the lock if the current client is the owner of the lock or
     * the ownership status cannot be determined (e.g., due to a network error).
     *
     * <p>
     * This method will not throw any exception, and do not block caller thread.
     */
    void close();
}

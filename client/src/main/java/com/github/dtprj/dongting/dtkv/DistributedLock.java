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
 * Distributed lock for dtkv.
 *
 * <p>
 * Unlike local lock, the distributed lock has lease time, the lock's ownership will be lost after the lease time.
 * Use isHeldByCurrentClient() method to check whether the current client is the owner of the lock.
 * Use setLockExpireListener(Runnable listener) to set a listener which will be called when the lock lease expires.
 *
 * @author huangli
 */
public interface DistributedLock {

    /**
     * Synchronously to acquire the lock, block caller thread and wait up to waitLockTimeoutMillis
     * if the lock is held by others.
     *
     * <p>
     * NOTICE: not forget check the return value, if false, means acquire lock failed.
     *
     * @param leaseMillis           The lease time of the lock, should be positive. If you don't know how to set it,
     *                              60000 (60 seconds) may be a good default value. The lease time starts when this
     *                              method invoked, that is, measured from the client side, not the server side.
     * @param waitLockTimeoutMillis max wait time to acquire the lock, none-negative, should be less or equal than
     *                              leaseMillis. If 0, return false immediately if the server tells the lock is held
     *                              by others. If positive, wait up to waitLockTimeoutMillis to acquire the lock.
     * @return whether acquire the lock successfully
     * @throws IllegalStateException if the lock is closed, or is already held by current client, or another
     *                               tryLock/unlock/updateLease operation is in progress.
     * @throws KvException           any biz exception
     * @throws NetException          any other exception such as network error, timeout, interrupted, etc.
     */
    boolean tryLock(long leaseMillis, long waitLockTimeoutMillis)
            throws IllegalStateException, KvException, NetException;

    /**
     * Asynchronously to acquire the lock, wait up to waitLockTimeoutMillis if the lock is held by others.
     * However, the caller thread will not be blocked, the callback will be called within waitLockTimeoutMillis.
     *
     * <p>
     * NOTICE: not forget check the return value in the callback, if false, means acquire lock failed.
     *
     * @param leaseMillis           The lease time of the lock, should be positive. If you don't know how to set it,
     *                              60000 (60 seconds) may be a good default value. The lease time starts when this
     *                              method invoked, that is, measured from the client side, not the server side.
     * @param waitLockTimeoutMillis max wait time to acquire the lock, none-negative, should be less or equal than
     *                              leaseMillis. If 0, return false immediately if the server tells the lock is held
     *                              by others. If positive, wait up to waitLockTimeoutMillis to acquire the lock.
     * @param callback              the async callback, generally run in bizExecutor of NioClient, it's best not to do
     *                              blocking operations in the callback.
     */
    void tryLock(long leaseMillis, long waitLockTimeoutMillis, FutureCallback<Boolean> callback);

    /**
     * Synchronously to release the lock, if the client is not the owner of the lock, do nothing.
     *
     * @throws IllegalStateException if the lock is closed, or another tryLock/unlock/updateLease operation is in progress.
     * @throws KvException           any biz exception
     * @throws NetException          any other exception such as network error, timeout, interrupted, etc.
     */
    void unlock() throws IllegalStateException, KvException, NetException;

    /**
     * Asynchronously to release the lock, if the client is not the owner of the lock, do nothing.
     *
     * @param callback the async callback, generally run in bizExecutor of NioClient, it's best not to do
     *                 blocking operations in the callback.
     */
    void unlock(FutureCallback<Void> callback);

    /**
     * Synchronously to update the lease time of the lock.
     *
     * @param newLeaseMillis the new lease time, should be positive, the lease time starts when this
     *                       method invoked, that is, measured from the client side, not the server side
     * @throws IllegalStateException if the lock is closed, or is not held by current client,
     *                               or another tryLock/unlock/updateLease operation is in progress,
     *                               or the lease has expired.
     * @throws KvException           any biz exception
     * @throws NetException          any other exception such as network error, timeout, interrupted, etc.
     */
    void updateLease(long newLeaseMillis) throws IllegalStateException, KvException, NetException;

    /**
     * Asynchronously to update the lease time of the lock.
     * @param newLeaseMillis the new lease time, should be positive, the lease time starts when this
     *                       method invoked, that is, measured from the client side, not the server side
     * @param callback       the async callback, generally run in bizExecutor of NioClient, it's best not to do
     *                       blocking operations in the callback.
     */
    void updateLease(long newLeaseMillis, FutureCallback<Void> callback);

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
     * Set the listener which will be called when the lock expires, this listener is running in a linearized mode,
     * don't do blocking operations in the listener.
     *
     * @param listener the listener
     */
    void setLockExpireListener(Runnable listener);

    /**
     * Safely close the lock, and remove it from KvClient.
     * After close, call createLock() in KvClient will get a new created instance.
     * After close, any method (except isHeldByCurrentClient and getLeaseRestMillis) invoked on this
     * instance will throw IllegalStateException.
     * This method try to asynchronously release the lock if the current client is the owner of the lock or
     * the ownership status cannot be determined (e.g., due to a network error).
     *
     * <p>
     * This method will not throw any exception, and do not block caller thread.
     */
    void close();
}

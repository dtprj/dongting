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
 * The lock operations are idempotent, if the client has held the lock, tryLock will return true directly. But it's
 * not reentrant, if the client has held the lock, and call unlock, the lock will be released directly.
 *
 * <p>
 * Unlike local lock, the distributed lock has lease time, the lock's ownership will be lost after the lease time.
 * Use isHeldByCurrentClient() method to check whether the current client is the owner of the lock.
 * Use setLockExpireListener(Runnable listener) to set a listener which will be called when the lock lease expires.
 * Use setAutoRenewLeaseIfOwner(boolean autoRenew) to automatically renew the lock lease before it expires if the current
 * client is the owner of the lock, but this re-new operation may be failed due to network error.
 *
 * @author huangli
 */
public interface DtKvLock extends AutoCloseable {

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
     * @throws KvException  any biz exception
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     */
    boolean tryLock(long leaseMillis, long waitLockTimeoutMillis) throws KvException, NetException;

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
     * @param callback              the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    void tryLock(long leaseMillis, long waitLockTimeoutMillis, FutureCallback<Boolean> callback);

    /**
     * Synchronously to release the lock, if the client is not the owner of the lock, do nothing.
     *
     * @throws KvException  any biz exception
     * @throws NetException any other exception such as network error, timeout, interrupted, etc.
     */
    void unlock() throws KvException, NetException;

    /**
     * Asynchronously to release the lock, if the client is not the owner of the lock, do nothing.
     *
     * @param callback the async callback will be called in bizExecutor (default) of NioClient or NioWorker thread.
     */
    void unlock(FutureCallback<Void> callback);

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
     * @return the remaining lease time in milliseconds
     */
    long getLeaseRestMillis();

    /**
     * Set the listener which will be called when the lock lease, the async callback will be called
     * in bizExecutor of NioClient.
     * @param listener the listener
     */
    void setLockExpireListener(Runnable listener);

    /**
     * Auto re-new the lock lease before it expires if the current client is the owner of the lock.
     * The task is scheduled in bizExecutor of NioClient.
     * Notice that the re-new operation may be failed due to network error.
     *
     * @param autoRenew true if enable auto update, false to disable it. if not set, the default is false.
     */
    void setAutoRenewLeaseIfOwner(boolean autoRenew);

    /**
     * Safely close the lock, try to release the lock if the current client is the owner of the lock or
     * the ownership status cannot be determined (e.g., due to a network error).
     *
     * <p>
     * This method will not throw any exception, so it's safe to call it in finally block.
     */
    @Override
    void close();
}

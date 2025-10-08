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

import com.github.dtprj.dongting.codec.DecoderCallbackCreator;
import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtBugException;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.FutureCallback;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.NetException;
import com.github.dtprj.dongting.net.NetTimeoutException;
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author huangli
 */
class DistributedLockImpl implements DistributedLock {
    private static final DtLog log = DtLogs.getLogger(DistributedLockImpl.class);

    // Lock states
    private static final int STATE_NOT_LOCKED = 0;  // not holding lock
    private static final int STATE_LOCKED = 1;      // confirmed holding lock
    private static final int STATE_UNKNOWN = 2;     // uncertain state (network issue, etc.)
    private static final int STATE_CLOSED = 3;      // lock instance closed

    /**
     * When receive Commands.DTKV_LOCK_PUSH, check if lockId is the same as the one in push, if not, ignore the push.
     */
    private final int lockId;

    /**
     * Each tryLock should increase the operationId by 1. When receive Commands.DTKV_LOCK_PUSH, check if
     * opId is the same as the one in push, if not, ignore the push.
     */
    private int opId;

    private final LockManager lockManager;
    final int groupId;
    final ByteArray key;
    private final ReentrantReadWriteLock opLock = new ReentrantReadWriteLock();

    // Current lock state
    private int state = STATE_NOT_LOCKED;
    private long leaseEndNanos;
    private long newLeaseEndNanos;

    private Op currentOp;

    private volatile Runnable expireListener;

    private int expireTaskEpoch;
    private ScheduledFuture<?> expireTask;

    DistributedLockImpl(int lockId, LockManager lockManager, int groupId, ByteArray key) {
        this.lockId = lockId;
        long now = System.nanoTime();
        this.opId = (int) now;
        this.newLeaseEndNanos = now;
        this.lockManager = lockManager;
        this.groupId = groupId;
        this.key = key;
    }

    private class Op implements Runnable, RpcCallback<Void> {
        final int taskOpId;
        final long waitLockTimeoutMillis;
        private final FutureCallback<?> callback;
        private ScheduledFuture<?> waitTimeoutTask;

        final int opType;
        private static final int OP_TYPE_TRY_LOCK = 1;
        private static final int OP_TYPE_UNLOCK = 2;
        private static final int OP_TYPE_RENEW = 3;

        boolean finish;
        private boolean called;

        private Object opResult;
        private Throwable opEx;

        Op(long leaseMillis, long waitLockTimeoutMillis, FutureCallback<Boolean> callback) {
            this.taskOpId = ++opId;
            newLeaseEndNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(leaseMillis);
            this.waitLockTimeoutMillis = waitLockTimeoutMillis;
            this.callback = callback;
            this.opType = OP_TYPE_TRY_LOCK;
        }

        Op(FutureCallback<Void> callback) {
            this.taskOpId = ++opId;
            this.waitLockTimeoutMillis = 0;
            this.callback = callback;
            this.opType = OP_TYPE_UNLOCK;
        }

        Op(long leaseMillis, FutureCallback<Void> callback) {
            this.taskOpId = ++opId;
            newLeaseEndNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(leaseMillis);
            this.waitLockTimeoutMillis = 0;
            this.callback = callback;
            this.opType = OP_TYPE_RENEW;
        }

        private void markFinishInLock(Object result, Throwable ex) {
            if (finish) {
                BugLog.log(new DtBugException("already finished"));
                return;
            }
            finish = true;
            if (currentOp == this) {
                currentOp = null;
            }
            if (waitTimeoutTask != null && !waitTimeoutTask.isDone()) {
                waitTimeoutTask.cancel(false);
            }
            waitTimeoutTask = null;

            this.opResult = result;
            this.opEx = ex;
        }

        @Override
        public void run() {
            // try lock timeout task
            opLock.writeLock().lock();
            try {
                if (finish) {
                    return;
                }
                String s = "tryLock " + key + " timeout after " + waitLockTimeoutMillis + "ms";
                markFinishInLock(null, new NetTimeoutException(s));
            } catch (Exception e) {
                BugLog.log(e);
            } finally {
                opLock.writeLock().unlock();
            }

            // call user callback outside lock
            invokeCallback();
        }

        void invokeCallback() {
            if (called) {
                BugLog.log(new DtBugException("already called"));
                return;
            }
            // call user callback outside lock, only once
            if (opEx == null) {
                //noinspection rawtypes,unchecked
                FutureCallback.callSuccess((FutureCallback) callback, opResult);
            } else {
                FutureCallback.callFail(callback, opEx);
            }
            called = true;
        }

        @Override
        public void call(ReadPacket<Void> p, Throwable ex) {
            opLock.writeLock().lock();
            try {
                if (finish) {
                    return;
                }
                String opTypeStr = opType == OP_TYPE_TRY_LOCK ? "tryLock"
                        : opType == OP_TYPE_UNLOCK ? "unlock" : "renew";
                if (state == STATE_CLOSED) {
                    markFinishInLock(null, new NetException("lock is closed"));
                } else if (ex != null) {
                    // try lock rpc response
                    log.error("{} rpc error", opTypeStr, ex);
                    markFinishInLock(null, ex);
                } else {
                    int bizCode = p.bizCode;
                    if (taskOpId != opId) {
                        // we check finish flag first, so this should not happen
                        markFinishInLock(null, new DtBugException("opId not match"));
                    } else if (opType == OP_TYPE_TRY_LOCK) {
                        if (bizCode == KvCodes.SUCCESS || bizCode == KvCodes.LOCK_BY_SELF) {
                            processLockResultAndMarkFinish(bizCode);
                        } else if (bizCode == KvCodes.LOCK_BY_OTHER || bizCode == KvCodes.NOT_FOUND) {
                            if (log.isDebugEnabled()) {
                                log.debug("tryLock get code {}, key: {}", KvCodes.toStr(bizCode), key);
                            }
                            if (waitLockTimeoutMillis == 0) {
                                // return immediately if waitLockTimeoutMillis is 0
                                markFinishInLock(Boolean.FALSE, null);
                            } else {
                                // wait push or timeout, return and don't fire callback now
                                return;
                            }
                        } else {
                            // Lock failed with other error
                            markFinishInLock(null, new KvException(bizCode));
                        }
                    } else if (opType == OP_TYPE_UNLOCK) {
                        if (p.bizCode == KvCodes.SUCCESS) {
                            leaseEndNanos = 0;
                            state = STATE_NOT_LOCKED;
                            if (log.isDebugEnabled()) {
                                log.debug("unlock success, key: {}", key);
                            }
                            markFinishInLock(null, null);
                        } else {
                            markFinishInLock(null, new KvException(p.bizCode));
                        }
                    } else {
                        // update lease
                        long now = System.nanoTime();
                        if (state != STATE_LOCKED || leaseEndNanos - now <= 0) {
                            markFinishInLock(null, new NetException("not held by current client"));
                        } else if (p.bizCode == KvCodes.SUCCESS) {
                            leaseEndNanos = newLeaseEndNanos;
                            cancelExpireTask();
                            expireTask = lockManager.executeService.schedule(() -> execExpireTask(expireTaskEpoch),
                                    leaseEndNanos - now, TimeUnit.NANOSECONDS);
                            markFinishInLock(null, null);
                        } else {
                            markFinishInLock(null, new KvException(p.bizCode));
                        }
                    }
                }
            } catch (Exception e) {
                log.error("unexpected error", e);
                if (!finish) {
                    markFinishInLock(null, e);
                }
            } finally {
                opLock.writeLock().unlock();
            }
            // call user callback outside lock, only once
            invokeCallback();
        }

        private void processLockResultAndMarkFinish(int bizCode) {
            if (opType != OP_TYPE_TRY_LOCK) {
                markFinishInLock(null, new DtBugException("not tryLock op"));
                return;
            }
            long now = System.nanoTime();
            if (newLeaseEndNanos - now <= 0) {
                markFinishInLock(null, new NetException("lease expired"));
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("tryLock success, key: {}, code={}", key, KvCodes.toStr(bizCode));
            }
            state = STATE_LOCKED;
            leaseEndNanos = newLeaseEndNanos;

            cancelExpireTask();
            expireTask = lockManager.executeService.schedule(() -> execExpireTask(expireTaskEpoch),
                    leaseEndNanos - now, TimeUnit.NANOSECONDS);
            markFinishInLock(Boolean.TRUE, null);
        }
    }

    @Override
    public boolean tryLock(long leaseMillis, long waitLockTimeoutMillis) throws KvException, NetException {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        tryLock(leaseMillis, waitLockTimeoutMillis, FutureCallback.fromFuture(future));

        return getFuture(future);
    }

    private <T> T getFuture(CompletableFuture<T> f) throws KvException, NetException {
        try {
            return f.get();
        } catch (InterruptedException e) {
            DtUtil.restoreInterruptStatus();
            throw new NetException("interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof KvException) {
                cause.addSuppressed(new NetException());
                throw (KvException) cause;
            } else if (cause instanceof NetException) {
                cause.addSuppressed(new NetException());
                throw (NetException) cause;
            } else {
                throw new NetException(cause);
            }
        }
    }

    @Override
    public void tryLock(long leaseMillis, long waitLockTimeoutMillis, FutureCallback<Boolean> callback) {
        DtUtil.checkPositive(leaseMillis, "leaseMillis");
        DtUtil.checkNotNegative(waitLockTimeoutMillis, "waitLockTimeoutMillis");
        if (leaseMillis < 1000) {
            log.warn("leaseMillis is too small: {}, key: {}", leaseMillis, key);
        }
        if (waitLockTimeoutMillis > leaseMillis) {
            throw new IllegalArgumentException("waitLockTimeoutMillis must be less than or equal to leaseMillis");
        }

        Throwable ex = tryLock0(leaseMillis, waitLockTimeoutMillis, callback);
        if (ex != null) {
            FutureCallback.callFail(callback, ex);
        }
    }

    private Throwable tryLock0(long leaseMillis, long waitLockTimeoutMillis, FutureCallback<Boolean> callback) {
        opLock.writeLock().lock();
        try {
            if (state == STATE_CLOSED) {
                return new IllegalStateException("lock is closed");
            }
            if (state == STATE_LOCKED) {
                return new IllegalStateException("already locked by current client");
            }
            // Wait for previous operation to complete
            if (currentOp != null) {
                return new IllegalStateException("operation in progress");
            }

            state = STATE_UNKNOWN;
            currentOp = new Op(leaseMillis, waitLockTimeoutMillis, callback);
            if (waitLockTimeoutMillis > 0) {
                currentOp.waitTimeoutTask = lockManager.executeService.schedule(currentOp,
                        waitLockTimeoutMillis, TimeUnit.MILLISECONDS);
            }

            // Create request with leaseMillis in value and operationId
            byte[] value = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(value);
            bb.putLong(leaseMillis); // big endian
            bb.putInt(lockId);
            bb.putInt(currentOp.taskOpId);

            KvReq req = new KvReq(groupId, key.getData(), value, waitLockTimeoutMillis);
            EncodableBodyWritePacket packet = new EncodableBodyWritePacket(Commands.DTKV_TRY_LOCK, req);

            lockManager.kvClient.raftClient.sendRequest(groupId, packet,
                    DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                    lockManager.kvClient.raftClient.createDefaultTimeout(), currentOp);
            return null;
        } catch (Throwable e) {
            log.error("tryLock error", e);
            if (currentOp != null) {
                // new operation created, need to mark finish
                currentOp.markFinishInLock(null, e);
            }
            return e;
        } finally {
            opLock.writeLock().unlock();
        }
    }

    private void execExpireTask(int expectEpoch) {
        opLock.writeLock().lock();
        Runnable listener = null;
        try {
            if (state != STATE_LOCKED) {
                return;
            }
            if (expectEpoch == expireTaskEpoch) {
                log.warn("lock expired without unlock or update lease, key: {}", key);
                state = STATE_NOT_LOCKED;
                leaseEndNanos = 0;
                expireTask = null;
                listener = expireListener;
            }
        } finally {
            opLock.writeLock().unlock();
        }
        if (listener != null) {
            try {
                listener.run();
            } catch (Exception e) {
                log.warn("lock expire listener error", e);
            }
        }
    }

    @Override
    public void unlock() throws KvException, NetException {
        CompletableFuture<Void> f = new CompletableFuture<>();
        unlock(FutureCallback.fromFuture(f));
        getFuture(f);
    }

    @Override
    public void unlock(FutureCallback<Void> callback) {
        Throwable ex = unlock0(callback);
        if (ex != null) {
            FutureCallback.callFail(callback, ex);
        }
    }

    private Throwable unlock0(FutureCallback<Void> callback) {
        Op oldOp;
        opLock.writeLock().lock();
        try {
            if (state == STATE_CLOSED) {
                return new IllegalStateException("lock is closed");
            }
            oldOp = currentOp;

            // mark current op as finished, so the unlock operation can be called safely after tryLock
            if (oldOp != null) {
                // currentOp is set to null in markFinishInLock
                oldOp.markFinishInLock(null, new NetException("canceled by unlock"));
            }
            cancelExpireTask();

            currentOp = new Op(callback);
            state = STATE_UNKNOWN;

            KvReq req = new KvReq(groupId, key.getData(), null);
            EncodableBodyWritePacket packet = new EncodableBodyWritePacket(Commands.DTKV_UNLOCK, req);

            lockManager.kvClient.raftClient.sendRequest(groupId, packet,
                    DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                    lockManager.kvClient.raftClient.createDefaultTimeout(), currentOp);
        } catch (Throwable e) {
            log.error("unlock error", e);
            if (currentOp != null) {
                currentOp.markFinishInLock(null, e);
            }
            return e;
        } finally {
            opLock.writeLock().unlock();
        }
        if (oldOp != null) {
            oldOp.invokeCallback();
        }
        return null;
    }

    private void cancelExpireTask() {
        expireTaskEpoch++;
        if (expireTask != null && !expireTask.isDone()) {
            expireTask.cancel(false);
        }
        expireTask = null;
    }

    @Override
    public void updateLease(long newLeaseMillis) {
        DtUtil.checkPositive(newLeaseMillis, "newLeaseMillis");
        if (newLeaseMillis < 1000) {
            log.warn("newLeaseMillis is too small: {}, key: {}", newLeaseMillis, key);
        }
        CompletableFuture<Void> f = new CompletableFuture<>();
        updateLease(newLeaseMillis, FutureCallback.fromFuture(f));
        getFuture(f);
    }

    @Override
    public void updateLease(long newLeaseMillis, FutureCallback<Void> callback) {
        Throwable e = updateLease0(newLeaseMillis, callback);
        if (e != null) {
            FutureCallback.callFail(callback, e);
        }
    }

    private Throwable updateLease0(long newLeaseMillis, FutureCallback<Void> callback) {
        opLock.writeLock().lock();
        try {
            if (state == STATE_CLOSED) {
                return new IllegalStateException("lock is closed");
            }
            if (state != STATE_LOCKED) {
                return new IllegalStateException("not locked by current client");
            }
            if (currentOp != null) {
                return new IllegalStateException("operation in progress");
            }

            long now = System.nanoTime();
            if (leaseEndNanos - now <= 0) {
                return new IllegalStateException("lease expired");
            }

            currentOp = new Op(newLeaseMillis, callback);

            KvReq req = new KvReq(groupId, key.getData(), null, newLeaseMillis);
            EncodableBodyWritePacket packet = new EncodableBodyWritePacket(Commands.DTKV_UPDATE_LOCK_LEASE, req);

            lockManager.kvClient.raftClient.sendRequest(groupId, packet,
                    DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                    lockManager.kvClient.raftClient.createDefaultTimeout(), currentOp);
            return null;
        } catch (Throwable e) {
            log.error("updateLease error", e);
            if (currentOp != null) {
                currentOp.markFinishInLock(null, e);
            }
            return e;
        } finally {
            opLock.writeLock().unlock();
        }
    }


    @Override
    public boolean isHeldByCurrentClient() {
        return getLeaseRestMillis() > 0;
    }

    @Override
    public long getLeaseRestMillis() {
        long rest;
        opLock.readLock().lock();
        try {
            if (state != STATE_LOCKED) {
                rest = 0;
            } else {
                rest = leaseEndNanos - System.nanoTime();
                if (rest < 0) {
                    rest = 0;
                }
            }
        } finally {
            opLock.readLock().unlock();
        }
        return TimeUnit.NANOSECONDS.toMillis(rest);
    }

    @Override
    public void setLockExpireListener(Runnable listener) {
        opLock.writeLock().lock();
        try {
            this.expireListener = listener;
        } finally {
            opLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        lockManager.removeLock(this);
    }

    void closeImpl() {
        Op oldOp = null;
        opLock.writeLock().lock();
        try {
            if (state == STATE_CLOSED) {
                return;
            }
            int oldState = state;
            state = STATE_CLOSED;
            oldOp = currentOp;
            if (oldOp != null) {
                oldOp.markFinishInLock(null, new NetException("canceled by close"));
            }
            cancelExpireTask();

            if ((oldState == STATE_LOCKED || oldState == STATE_UNKNOWN) && (newLeaseEndNanos - System.nanoTime() > 0)) {
                KvReq req = new KvReq(groupId, key.getData(), null);
                EncodableBodyWritePacket packet = new EncodableBodyWritePacket(Commands.DTKV_UNLOCK, req);
                lockManager.kvClient.raftClient.sendRequest(groupId, packet,
                        DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                        lockManager.kvClient.raftClient.createDefaultTimeout(), null);
            }
        } catch (Exception e) {
            log.error("lock close error", e);
        } finally {
            opLock.writeLock().unlock();
        }
        if (oldOp != null) {
            oldOp.invokeCallback();
        }
    }

    void processLockPush(int bizCode, byte[] value) {
        ByteBuffer buf = ByteBuffer.wrap(value);
        int pushLockId = buf.getInt();
        int pushOpId = buf.getInt();

        Op oldOp = null;
        opLock.writeLock().lock();
        try {
            if (state == STATE_CLOSED) {
                log.info("ignore lock push because lock is closed. key: {}", key);
                return;
            }
            if (pushOpId != opId || pushLockId != lockId) {
                log.info("ignore lock push. key: {}, pushOpId: {}, opId: {}, pushLockId: {}, lockId: {}",
                        key, pushOpId, opId, pushLockId, lockId);
                return;
            }
            oldOp = currentOp;
            if (oldOp == null) {
                log.warn("ignore lock push because no current op. key: {}", key);
                return;
            }
            oldOp.processLockResultAndMarkFinish(bizCode);
        } catch (Exception e) {
            BugLog.log(e);
        } finally {
            opLock.writeLock().unlock();
        }

        if (oldOp != null) {
            oldOp.invokeCallback();
        }

    }
}

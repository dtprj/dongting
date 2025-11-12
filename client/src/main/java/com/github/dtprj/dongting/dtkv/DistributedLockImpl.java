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
import com.github.dtprj.dongting.net.ReadPacket;
import com.github.dtprj.dongting.net.RpcCallback;
import com.github.dtprj.dongting.net.WritePacket;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author huangli
 */
public class DistributedLockImpl implements DistributedLock {
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

    Runnable expireListener;
    final LockManager lockManager;
    final int groupId;
    final ByteArray key;
    private final ReentrantLock opLock = new ReentrantLock();

    /**
     * Each rpc should increase the operationId by 1. When receive Commands.DTKV_LOCK_PUSH, check if
     * opId is the same as the one in push, if not, ignore the push.
     */
    private int opId;
    private int leaseId;

    // Current lock state
    private int state = STATE_NOT_LOCKED;
    private volatile long leaseEndNanos;
    private long newLeaseEndNanos;

    private Op currentOp;

    private ScheduledFuture<?> expireTask;

    protected DistributedLockImpl(int lockId, LockManager lockManager, int groupId, ByteArray key, Runnable expireListener) {
        this.lockId = lockId;
        this.lockManager = lockManager;
        this.groupId = groupId;
        this.key = key;
        this.expireListener = expireListener;

        resetLeaseEndNanos();
    }

    private void resetLeaseEndNanos() {
        long now = System.nanoTime();
        this.leaseEndNanos = now - 10_000_000_000L;
        this.newLeaseEndNanos = leaseEndNanos;
    }

    private class Op implements RpcCallback<Void> {
        private int taskOpId;
        private final FutureCallback<?> callback;
        private final long tryLockTimeoutMillis;
        private final long leaseMillis;
        private ScheduledFuture<?> tryLockTimeoutTask;

        final int opType;
        private static final int OP_TYPE_TRY_LOCK = 1;
        private static final int OP_TYPE_UNLOCK = 2;
        private static final int OP_TYPE_RENEW = 3;

        boolean finish;
        private boolean called;

        private Object opResult;
        private Throwable opEx;

        Op(int opType, long leaseMillis, long tryLockTimeoutMillis,FutureCallback<?> callback) {
            this.tryLockTimeoutMillis = tryLockTimeoutMillis;
            this.leaseMillis = leaseMillis;
            this.callback = callback;
            this.opType = opType;
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
            if (tryLockTimeoutTask != null && !tryLockTimeoutTask.isDone()) {
                tryLockTimeoutTask.cancel(false);
            }
            tryLockTimeoutTask = null;

            this.opResult = result;
            this.opEx = ex;
        }

        public void makeTryLockTimeout() {
            // try lock timeout task
            opLock.lock();
            try {
                if (finish) {
                    return;
                }
                markFinishInLock(Boolean.FALSE, null);
            } catch (Exception e) {
                BugLog.log(e);
                return;
            } finally {
                opLock.unlock();
            }

            log.info("tryLock timeout after {} ms, key: {}", tryLockTimeoutMillis, key);

            // call user callback outside lock
            invokeCallback();
        }

        public void invokeCallback() {
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
            opLock.lock();
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
                            processLockResultAndMarkFinish(bizCode, 0);
                        } else if (bizCode == KvCodes.LOCK_BY_OTHER) {
                            if (log.isDebugEnabled()) {
                                log.debug("tryLock get code {}, key: {}", KvCodes.toStr(bizCode), key);
                            }
                            if (tryLockTimeoutMillis == 0) {
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
                        if (p.bizCode == KvCodes.SUCCESS || p.bizCode == KvCodes.LOCK_BY_OTHER || p.bizCode == KvCodes.NOT_FOUND) {
                            resetLeaseEndNanos();
                            state = STATE_NOT_LOCKED;
                            if (p.bizCode == KvCodes.SUCCESS) {
                                if (log.isDebugEnabled()) {
                                    log.debug("unlock success, key: {}", key);
                                }
                            } else {
                                log.warn("unlock get code {}, key: {}", KvCodes.toStr(p.bizCode), key);
                            }
                            markFinishInLock(null, null);
                        } else {
                            markFinishInLock(null, new KvException(p.bizCode));
                        }
                    } else {
                        // update lease
                        long now = System.nanoTime();
                        if (state != STATE_LOCKED || leaseEndNanos - now <= 0 || newLeaseEndNanos - now <= 0) {
                            markFinishInLock(null, new NetException("not held by current client"));
                        } else if (p.bizCode == KvCodes.SUCCESS) {
                            leaseEndNanos = newLeaseEndNanos;
                            leaseId++;
                            cancelExpireTask();
                            scheduleExpireTask(leaseEndNanos - now);
                            markFinishInLock(null, null);
                        } else {
                            markFinishInLock(null, new KvException(p.bizCode));
                        }
                    }
                }
            } catch (Throwable e) {
                log.error("unexpected error", e);
                if (!finish) {
                    markFinishInLock(null, e);
                }
            } finally {
                opLock.unlock();
            }
            // call user callback outside lock, only once
            invokeCallback();
        }

        private void processLockResultAndMarkFinish(int bizCode, long serverSideWaitNanos) {
            if (opType != OP_TYPE_TRY_LOCK) {
                markFinishInLock(null, new DtBugException("not tryLock op"));
                return;
            }
            newLeaseEndNanos += serverSideWaitNanos;
            long now = System.nanoTime();
            if (newLeaseEndNanos - now <= 0) {
                log.warn("tryLock success in server side, but already expires locally." +
                        " leaseMillis={}, serverSideWaitMillis={}, key={}",
                        leaseMillis, serverSideWaitNanos / 1_000_000, key);
                markFinishInLock(Boolean.FALSE, null);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("tryLock success, key: {}, code={}", key, KvCodes.toStr(bizCode));
            }
            state = STATE_LOCKED;
            leaseId++;
            leaseEndNanos = newLeaseEndNanos;

            if (expireTask != null) {
                BugLog.getLog().error("expireTask already exists, key: {}", key);
                cancelExpireTask();
            }
            scheduleExpireTask(leaseEndNanos - now);
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
                throw new KvException(((KvException) cause).getCode(), cause);
            } else if (cause instanceof IllegalStateException) {
                throw (IllegalStateException) cause;
            } else {
                throw new NetException(cause);
            }
        }
    }

    @Override
    public void tryLock(long leaseMillis, long waitLockTimeoutMillis, FutureCallback<Boolean> callback) {
        DtUtil.checkNotNegative(waitLockTimeoutMillis, "waitLockTimeoutMillis");
        checkLeaseMillis(leaseMillis);
        if (waitLockTimeoutMillis > leaseMillis) {
            throw new IllegalArgumentException("waitLockTimeoutMillis must be less than or equal to leaseMillis");
        }

        Op op = new Op(Op.OP_TYPE_TRY_LOCK, leaseMillis, waitLockTimeoutMillis, callback);
        boolean ok = false;
        opLock.lock();
        try {
            tryLock0(leaseMillis, waitLockTimeoutMillis, op);
            ok = true;
            currentOp = op;
        } catch (Throwable e) {
            op.markFinishInLock(null, e);
        } finally {
            opLock.unlock();
        }
        if (!ok) {
            op.invokeCallback();
        }
    }

    private void tryLock0(long leaseMillis, long waitLockTimeoutMillis, Op op) {
        if (state == STATE_CLOSED) {
            throw new IllegalStateException("lock is closed");
        }
        if (state == STATE_LOCKED) {
            throw new IllegalStateException("already locked by current client");
        }
        // Wait for previous operation to complete
        if (currentOp != null) {
            throw new IllegalStateException("operation in progress");
        }

        op.taskOpId = ++opId;
        state = STATE_UNKNOWN;

        if (waitLockTimeoutMillis > 0) {
            op.tryLockTimeoutTask = lockManager.schedule(op::makeTryLockTimeout, waitLockTimeoutMillis, TimeUnit.MILLISECONDS);
        }

        // Create request with leaseMillis in value and operationId
        byte[] value = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(value);
        bb.putLong(leaseMillis); // big endian
        bb.putInt(lockId);
        bb.putInt(op.taskOpId);

        KvReq req = new KvReq(groupId, key.getData(), value, waitLockTimeoutMillis);
        EncodableBodyWritePacket packet = new EncodableBodyWritePacket(Commands.DTKV_TRY_LOCK, req);
        packet.acquirePermitNoWait = true;

        newLeaseEndNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(leaseMillis);
        sendRpc(packet, op);
    }

    // for unit test mock
    protected void sendRpc(WritePacket packet, RpcCallback<Void> callback) {
        lockManager.kvClient.raftClient.sendRequest(groupId, packet,
                DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                lockManager.kvClient.raftClient.createDefaultTimeout(), callback);
    }

    private void scheduleExpireTask(long delayNanos) {
        int expectLeaseId = leaseId;
        expireTask = lockManager.schedule(() -> execExpireTask(expectLeaseId), delayNanos, TimeUnit.NANOSECONDS);
    }

    private void execExpireTask(int expectLeaseId) {
        opLock.lock();
        try {
            expireTask = null;
            if (state != STATE_LOCKED) {
                return;
            }
            if (expectLeaseId != leaseId) {
                return;
            }
            log.warn("lock expired without unlock or update lease, key: {}", key);
            state = STATE_NOT_LOCKED;
            resetLeaseEndNanos();
            if (expireListener != null) {
                try {
                    expireListener.run();
                } catch (Throwable e) {
                    log.warn("lock expire listener error", e);
                }
            }
        } finally {
            opLock.unlock();
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
        Op op = new Op(Op.OP_TYPE_UNLOCK, 0, 0, callback);
        Op oldOp = null;
        boolean shouldInvokeCallback;
        opLock.lock();
        try {
            if (state == STATE_CLOSED) {
                throw new IllegalStateException("lock is closed");
            }
            if (state == STATE_NOT_LOCKED) {
                op.markFinishInLock(null, null);
                shouldInvokeCallback = true;
            } else {
                oldOp = unlock0(op);
                currentOp = op;
                shouldInvokeCallback = false;
            }
        } catch (Throwable e) {
            op.markFinishInLock(null, e);
            shouldInvokeCallback = true;
        } finally {
            opLock.unlock();
        }
        if (oldOp != null) {
            oldOp.invokeCallback();
        }
        if (shouldInvokeCallback) {
            op.invokeCallback();
        }
    }

    private Op unlock0(Op op) {
        Op oldOp;
        boolean makeOldOpFinish = false;
        oldOp = currentOp;

        // mark current op as finished, so the unlock operation can be called safely after tryLock
        if (oldOp != null && !oldOp.finish) {
            // currentOp is set to null in markFinishInLock
            oldOp.markFinishInLock(null, new NetException("canceled by unlock"));
            makeOldOpFinish = true;
        }
        cancelExpireTask();

        op.taskOpId = ++opId;
        state = STATE_UNKNOWN;
        resetLeaseEndNanos();

        KvReq req = new KvReq(groupId, key.getData(), null);
        EncodableBodyWritePacket packet = new EncodableBodyWritePacket(Commands.DTKV_UNLOCK, req);
        packet.acquirePermitNoWait = true;

        sendRpc(packet, op);
        return makeOldOpFinish ? oldOp : null;
    }

    private void cancelExpireTask() {
        if (expireTask != null && !expireTask.isDone()) {
            expireTask.cancel(false);
        }
        expireTask = null;
    }

    @Override
    public void updateLease(long newLeaseMillis) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        updateLease(newLeaseMillis, FutureCallback.fromFuture(f));
        getFuture(f);
    }

    @Override
    public void updateLease(long leaseMillis, FutureCallback<Void> callback) {
        checkLeaseMillis(leaseMillis);
        Op op = new Op(Op.OP_TYPE_RENEW, leaseMillis, 0, callback);
        boolean ok = false;
        opLock.lock();
        try {
            updateLease0(leaseMillis, op);
            ok = true;
            currentOp = op;
        } catch (Throwable e) {
            op.markFinishInLock(null, e);
        } finally {
            opLock.unlock();
        }
        if (!ok) {
            op.invokeCallback();
        }
    }

    private void updateLease0(long newLeaseMillis, Op op) {
        if (state == STATE_CLOSED) {
            throw new IllegalStateException("lock is closed");
        }
        if (state != STATE_LOCKED) {
            throw new IllegalStateException("not locked by current client");
        }
        if (currentOp != null) {
            throw new IllegalStateException("operation in progress");
        }

        long now = System.nanoTime();
        if (leaseEndNanos - now <= 0) {
            throw new IllegalStateException("lease expired");
        }

        op.taskOpId = ++opId;

        KvReq req = new KvReq(groupId, key.getData(), null, newLeaseMillis);
        EncodableBodyWritePacket packet = new EncodableBodyWritePacket(Commands.DTKV_UPDATE_LOCK_LEASE, req);
        packet.acquirePermitNoWait = true;

        newLeaseEndNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(newLeaseMillis);
        sendRpc(packet, op);
    }

    @Override
    public boolean isHeldByCurrentClient() {
        return getLeaseRestMillis() > 0;
    }

    @Override
    public long getLeaseRestMillis() {
        long rest = leaseEndNanos - System.nanoTime();
        if (rest < 0) {
            rest = 0;
        }
        return TimeUnit.NANOSECONDS.toMillis(rest);
    }

    @Override
    public void close() {
        lockManager.removeLock(this);
    }

    void closeImpl() {
        Op oldOp = null;
        boolean invokeOldOpCallback = false;
        opLock.lock();
        try {
            if (state == STATE_CLOSED) {
                return;
            }
            int oldState = state;
            state = STATE_CLOSED;
            oldOp = currentOp;
            if (oldOp != null && !oldOp.finish) {
                oldOp.markFinishInLock(null, new NetException("canceled by close"));
                invokeOldOpCallback = true;
            }
            cancelExpireTask();

            boolean needSendUnlock = (oldState == STATE_LOCKED || oldState == STATE_UNKNOWN)
                    && (newLeaseEndNanos - System.nanoTime() > 0);
            resetLeaseEndNanos();
            if (needSendUnlock) {
                KvReq req = new KvReq(groupId, key.getData(), null);
                EncodableBodyWritePacket packet = new EncodableBodyWritePacket(Commands.DTKV_UNLOCK, req);
                packet.acquirePermitNoWait = true;
                sendRpc(packet, null);
            }
        } catch (Exception e) {
            log.error("lock close error", e);
        } finally {
            opLock.unlock();
        }
        if (invokeOldOpCallback) {
            oldOp.invokeCallback();
        }
    }

    void processLockPush(int bizCode, byte[] value, long serverSideWaitNanos) {
        if (value.length < 16) {
            log.warn("ignore lock push because value length is invalid: {}, key: {}", value.length, key);
            return;
        }
        ByteBuffer buf = ByteBuffer.wrap(value);
        int pushLockId = buf.getInt(8);
        int pushOpId = buf.getInt(12);

        Op oldOp = null;
        opLock.lock();
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
            oldOp.processLockResultAndMarkFinish(bizCode, serverSideWaitNanos);
        } catch (Exception e) {
            BugLog.log(e);
        } finally {
            opLock.unlock();
        }

        if (oldOp != null) {
            oldOp.invokeCallback();
        }
    }

    static void checkLeaseMillis(long leaseMillis) {
        DtUtil.checkPositive(leaseMillis, "leaseMillis");
        if (leaseMillis < 1000) {
            log.warn("leaseMillis is too small: {}", leaseMillis);
        }
    }
}

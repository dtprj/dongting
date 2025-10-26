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
import com.github.dtprj.dongting.net.WritePacket;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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

    final LockManager lockManager;
    final int groupId;
    final ByteArray key;

    /**
     * Each rpc should increase the operationId by 1.
     */
    private int rpcOpId;

    private int leaseEpoch;

    // Current lock state
    private int state = STATE_NOT_LOCKED;
    private volatile long leaseEndNanos;
    private long newLeaseEndNanos;

    private Op currentOp;
    private final LinearQueue linearQueue = new LinearQueue();

    private volatile Runnable expireListener;

    private ScheduledFuture<?> expireTask;

    DistributedLockImpl(int lockId, LockManager lockManager, int groupId, ByteArray key) {
        this.lockId = lockId;
        this.lockManager = lockManager;
        this.groupId = groupId;
        this.key = key;

        this.rpcOpId = 0;
        this.leaseEpoch = 0;
        resetLeaseEndNanos();
    }

    private void resetLeaseEndNanos() {
        long nowNanos = System.nanoTime();
        this.newLeaseEndNanos = leaseEndNanos;
        this.leaseEndNanos = nowNanos - 1_000_000_000;
    }

    static class LinearQueue {
        private final LinkedList<Runnable> runnableQueue = new LinkedList<>();
        private boolean running = false;

        public void linearRun(boolean addFirst, Runnable nextTask) {
            Objects.requireNonNull(nextTask);
            boolean firstLoop = true;
            try {
                while (true) {
                    Runnable r;

                    synchronized (this) {
                        if (firstLoop) {
                            if (running) {
                                if (addFirst) {
                                    runnableQueue.addFirst(nextTask);
                                } else {
                                    runnableQueue.addLast(nextTask);
                                }
                                return;
                            } else {
                                running = true;
                                firstLoop = false;
                                r = nextTask;
                            }
                        } else {
                            r = runnableQueue.pollFirst();
                        }
                        if (r == null) {
                            running = false;
                            return;
                        }
                    } // end synchronized block

                    // run outside synchronized block
                    r.run(); // should not throw exception
                }
            } catch (RuntimeException | Error e) {
                // assert false
                synchronized (this) {
                    running = false;
                }
                log.error("LinearQueue task error", e);
                throw e;
            }
        }
    }

    private class Op implements Runnable, RpcCallback<Void> {
        private final long leaseMillis;
        private final long opTimeoutMillis;
        private final FutureCallback<?> callback;
        private final int opType;

        private int taskOpId;
        private ScheduledFuture<?> tryLockTimeoutTask;

        private static final int OP_TYPE_TRY_LOCK = 1;
        private static final int OP_TYPE_UNLOCK = 2;
        private static final int OP_TYPE_RENEW = 3;

        private boolean finish;

        Op(int opType, long leaseMillis, long opTimeoutMillis, FutureCallback<?> callback) {
            this.leaseMillis = leaseMillis;
            this.opTimeoutMillis = opTimeoutMillis;
            this.callback = callback;
            this.opType = opType;
        }

        @Override
        public void run() {
            try {
                if (state == STATE_CLOSED) {
                    throw new IllegalStateException("lock is closed");
                }
                // Wait for previous operation to complete
                if (currentOp != null) {
                    throw new IllegalStateException("operation in progress");
                }
                currentOp = this;

                this.taskOpId = ++rpcOpId;

                WritePacket packet;
                if (opType == OP_TYPE_TRY_LOCK) {
                    packet = tryLock0(leaseMillis, opTimeoutMillis);
                } else if (opType == OP_TYPE_UNLOCK) {
                    packet = unlock0();
                } else {
                    packet = updateLease0(leaseMillis);
                }
                lockManager.kvClient.raftClient.sendRequest(groupId, packet,
                        DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                        lockManager.kvClient.raftClient.createDefaultTimeout(), currentOp);
            } catch (Throwable e) {
                finish(null, e);
            }
        }

        private void finish(Object result, Throwable ex) {
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

            // call user callback outside lock, only once
            if (ex == null) {
                //noinspection rawtypes,unchecked
                FutureCallback.callSuccess((FutureCallback) callback, result);
            } else {
                FutureCallback.callFail(callback, ex);
            }
        }

        public void makeTryLockTimeout() {
            linearQueue.linearRun(true, () -> {
                try {
                    // try lock timeout task
                    if (finish) {
                        return;
                    }
                    String s = "tryLock " + key + " timeout after " + opTimeoutMillis + "ms";
                    finish(null, new NetTimeoutException(s));
                } catch (Throwable e) {
                    log.error("make tryLock timeout error", e);
                }
            });
        }

        @Override
        public void call(ReadPacket<Void> p, Throwable ex) {
            linearQueue.linearRun(false, () -> rpcCallbackRunedInLinearQueue(p, ex));
        }

        private void rpcCallbackRunedInLinearQueue(ReadPacket<Void> p, Throwable ex) {
            try {
                if (finish) {
                    return;
                }
                String opTypeStr = opType == OP_TYPE_TRY_LOCK ? "tryLock"
                        : opType == OP_TYPE_UNLOCK ? "unlock" : "renew";
                if (state == STATE_CLOSED) {
                    finish(null, new NetException("lock is closed"));
                } else if (ex != null) {
                    // try lock rpc response
                    log.error("{} rpc error", opTypeStr, ex);
                    finish(null, ex);
                } else {
                    int bizCode = p.bizCode;
                    if (taskOpId != rpcOpId) {
                        // we check finish flag and state first, so this should not happen
                        finish(null, new DtBugException("opId not match"));
                    } else if (opType == OP_TYPE_TRY_LOCK) {
                        if (bizCode == KvCodes.SUCCESS || bizCode == KvCodes.LOCK_BY_SELF) {
                            processLockResultAndMarkFinish(bizCode);
                        } else if (bizCode == KvCodes.LOCK_BY_OTHER) {
                            if (log.isDebugEnabled()) {
                                log.debug("tryLock get code {}, key: {}", KvCodes.toStr(bizCode), key);
                            }
                            if (opTimeoutMillis == 0) {
                                // return immediately if waitLockTimeoutMillis is 0
                                finish(Boolean.FALSE, null);
                            } else {
                                // wait push or timeout, return and don't fire callback now
                                //noinspection UnnecessaryReturnStatement
                                return;
                            }
                        } else {
                            // Lock failed with other error
                            finish(null, new KvException(bizCode));
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
                                log.warn("unlock returned code {}, key: {}", KvCodes.toStr(p.bizCode), key);
                            }
                            finish(null, null);
                        } else {
                            finish(null, new KvException(p.bizCode));
                        }
                    } else {
                        // update lease
                        long now = System.nanoTime();
                        if (state != STATE_LOCKED || leaseEndNanos - now <= 0) {
                            finish(null, new NetException("not held by current client"));
                        } else if (p.bizCode == KvCodes.SUCCESS) {
                            leaseEpoch++;
                            leaseEndNanos = newLeaseEndNanos;
                            cancelExpireTask();
                            scheduleExpireTask(leaseEndNanos - now);
                            finish(null, null);
                        } else {
                            finish(null, new KvException(p.bizCode));
                        }
                    }
                }
            } catch (Throwable e) {
                log.error("unexpected error", e);
                if (!finish) {
                    finish(null, e);
                }
            }
        }

        private void processLockResultAndMarkFinish(int bizCode) {
            if (opType != OP_TYPE_TRY_LOCK) {
                finish(null, new DtBugException("not tryLock op"));
                return;
            }
            long now = System.nanoTime();
            if (newLeaseEndNanos - now <= 0) {
                finish(null, new NetException("lease expired"));
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("tryLock success, key: {}, code={}", key, KvCodes.toStr(bizCode));
            }
            state = STATE_LOCKED;
            leaseEpoch++;
            leaseEndNanos = newLeaseEndNanos;

            scheduleExpireTask(leaseEndNanos - now);
            finish(Boolean.TRUE, null);
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
                throw new KvException(((KvException) cause).getCode(), e);
            } else if (cause instanceof IllegalStateException) {
                throw new IllegalStateException(cause.getMessage(), e);
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

        linearQueue.linearRun(false, new Op(Op.OP_TYPE_TRY_LOCK, leaseMillis, waitLockTimeoutMillis, callback));
    }

    private WritePacket tryLock0(long leaseMillis, long waitLockTimeoutMillis) {
        if (state == STATE_LOCKED) {
            throw new IllegalStateException("already locked by current client");
        }
        newLeaseEndNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(leaseMillis);
        state = STATE_UNKNOWN;

        if (waitLockTimeoutMillis > 0) {
            Op op = currentOp;
            op.tryLockTimeoutTask = lockManager.executeService.schedule(op::makeTryLockTimeout,
                    waitLockTimeoutMillis, TimeUnit.MILLISECONDS);
        }

        // Create request with leaseMillis in value and operationId
        byte[] value = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(value);
        bb.putLong(leaseMillis); // big endian
        bb.putInt(lockId);
        bb.putInt(currentOp.taskOpId);

        KvReq req = new KvReq(groupId, key.getData(), value, waitLockTimeoutMillis);
        return new EncodableBodyWritePacket(Commands.DTKV_TRY_LOCK, req);
    }

    private void scheduleExpireTask(long delayNanos) {
        if (expireTask != null) {
            BugLog.getLog().error("expireTask already exists, key: {}", key);
        }
        int expectEpoch = leaseEpoch;
        expireTask = lockManager.executeService.schedule(() -> execExpireTask(expectEpoch),
                delayNanos, TimeUnit.NANOSECONDS);
    }

    private void execExpireTask(int expectLeaseEpoch) {
        linearQueue.linearRun(true, () -> {
            try {
                if (state != STATE_LOCKED) {
                    return;
                }
                if (expectLeaseEpoch != leaseEpoch) {
                    return;
                }
                log.warn("lock expired without unlock or update lease, key: {}", key);
                rpcOpId++;
                state = STATE_NOT_LOCKED;
                currentOp = null;
                resetLeaseEndNanos();
                expireTask = null;
                if (expireListener != null) {
                    expireListener.run();
                }
            } catch (Throwable e) {
                log.warn("lock expire listener error", e);
            }
        });
    }

    @Override
    public void unlock() throws KvException, NetException {
        CompletableFuture<Void> f = new CompletableFuture<>();
        unlock(FutureCallback.fromFuture(f));
        getFuture(f);
    }

    @Override
    public void unlock(FutureCallback<Void> callback) {
        linearQueue.linearRun(false, new Op(Op.OP_TYPE_UNLOCK, 0, 0, callback));
    }

    private WritePacket unlock0() {
        cancelExpireTask();
        state = STATE_UNKNOWN;
        resetLeaseEndNanos();
        KvReq req = new KvReq(groupId, key.getData(), null);
        return new EncodableBodyWritePacket(Commands.DTKV_UNLOCK, req);
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
    public void updateLease(long newLeaseMillis, FutureCallback<Void> callback) {
        DtUtil.checkPositive(newLeaseMillis, "newLeaseMillis");
        if (newLeaseMillis < 1000) {
            log.warn("newLeaseMillis is too small: {}, key: {}", newLeaseMillis, key);
        }
        linearQueue.linearRun(false, new Op(Op.OP_TYPE_RENEW, newLeaseMillis, 0, callback));
    }

    private WritePacket updateLease0(long newLeaseMillis) {
        if (state != STATE_LOCKED) {
            throw new IllegalStateException("not locked by current client");
        }
        long now = System.nanoTime();
        if (leaseEndNanos - now <= 0) {
            throw new IllegalStateException("lease expired");
        }

        newLeaseEndNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(newLeaseMillis);
        KvReq req = new KvReq(groupId, key.getData(), null, newLeaseMillis);
        return new EncodableBodyWritePacket(Commands.DTKV_UPDATE_LOCK_LEASE, req);
    }


    @Override
    public boolean isHeldByCurrentClient() {
        return getLeaseRestMillis() > 0;
    }

    @Override
    public long getLeaseRestMillis() {
        long n = leaseEndNanos - System.nanoTime();
        if (n < 0) {
            n = 0;
        }
        return TimeUnit.NANOSECONDS.toMillis(n);
    }

    @Override
    public void setLockExpireListener(Runnable listener) {
        this.expireListener = listener;
    }

    @Override
    public void close() {
        lockManager.removeLock(this);
    }

    void closeImpl() {
        linearQueue.linearRun(true, () -> {
            try {
                if (state == STATE_CLOSED) {
                    return;
                }
                int oldState = state;
                state = STATE_CLOSED;
                cancelExpireTask();
                resetLeaseEndNanos();

                if (currentOp != null) {
                    currentOp.finish(null, new NetException("canceled by close"));
                }

                if ((oldState == STATE_LOCKED || oldState == STATE_UNKNOWN) && (newLeaseEndNanos - System.nanoTime() > 0)) {
                    KvReq req = new KvReq(groupId, key.getData(), null);
                    EncodableBodyWritePacket packet = new EncodableBodyWritePacket(Commands.DTKV_UNLOCK, req);
                    lockManager.kvClient.raftClient.sendRequest(groupId, packet,
                            DecoderCallbackCreator.VOID_DECODE_CALLBACK_CREATOR,
                            lockManager.kvClient.raftClient.createDefaultTimeout(), null);
                }
            } catch (Throwable e) {
                log.error("lock close error", e);
            }
        });
    }

    void processLockPush(int bizCode, byte[] value) {
        ByteBuffer buf = ByteBuffer.wrap(value);
        int pushLockId = buf.getInt();
        int pushOpId = buf.getInt();

        linearQueue.linearRun(false, () -> {
            try {
                if (state == STATE_CLOSED) {
                    log.info("ignore lock push because lock is closed. key: {}", key);
                    return;
                }
                if (pushOpId != rpcOpId || pushLockId != lockId) {
                    log.info("ignore lock push. key: {}, pushOpId: {}, opId: {}, pushLockId: {}, lockId: {}",
                            key, pushOpId, rpcOpId, pushLockId, lockId);
                    return;
                }
                if (currentOp == null) {
                    log.warn("ignore lock push because no current op. key: {}", key);
                    return;
                }
                currentOp.processLockResultAndMarkFinish(bizCode);
            } catch (Throwable e) {
                BugLog.log(e);
            }
        });
    }
}

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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class AsyncRetryTask<T> implements BiConsumer<T, Throwable> {
    private static final DtLog log = DtLogs.getLogger(AsyncRetryTask.class);
    private final Supplier<CompletableFuture<T>> futureSupplier;
    private final RaftStatus raftStatus;
    private final String errorMsg;

    private final CompletableFuture<T> finalResult = new CompletableFuture<>();
    private int failCount;

    public AsyncRetryTask(Supplier<CompletableFuture<T>> futureSupplier, RaftStatus raftStatus, String errorMsg) {
        this.futureSupplier = futureSupplier;
        this.raftStatus = raftStatus;
        this.errorMsg = errorMsg;
    }

    public void exec() {
        futureSupplier.get().whenComplete(this);
    }

    @Override
    public void accept(T t, Throwable ex) {
        if (ex == null) {
            finalResult.complete(t);
            if (failCount > 0) {
                raftStatus.setError(false);
            }
        } else {
            failCount++;
            log.error(errorMsg, ex);
            raftStatus.setError(true);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.warn("Interrupted when retry");
                // finalResult never complete
            }
        }
    }

    public CompletableFuture<T> getFinalResult() {
        return finalResult;
    }
}

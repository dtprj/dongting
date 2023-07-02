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

import com.github.dtprj.dongting.common.Pair;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * @author huangli
 */
class FutureEventSource {
    private final Executor executor;
    protected final LinkedList<Pair<CompletableFuture<Void>, Supplier<Boolean>>> listeners = new LinkedList<>();

    public FutureEventSource(Executor executor) {
        this.executor = executor;
    }

    public CompletableFuture<Void> registerInOtherThreads(Supplier<Boolean> predicate) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        executor.execute(() -> {
            if (predicate.get()) {
                f.complete(null);
            } else {
                Pair<CompletableFuture<Void>, Supplier<Boolean>> pair = new Pair<>(f, predicate);
                listeners.add(pair);
            }
        });
        return f;
    }

    protected void fireInExecutorThread() {
        for (Iterator<Pair<CompletableFuture<Void>, Supplier<Boolean>>> it = listeners.iterator(); it.hasNext(); ) {
            Pair<CompletableFuture<Void>, Supplier<Boolean>> pair = it.next();
            if (pair.getRight().get()) {
                pair.getLeft().complete(null);
                it.remove();
            }
        }
    }
}

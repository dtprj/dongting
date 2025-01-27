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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.common.FutureCallback;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

/**
 * @author huangli
 */
@SuppressWarnings("Convert2Diamond")
public interface RpcCallback<T> extends FutureCallback<ReadPacket<T>> {
    static <T> RpcCallback<T> fromUnwrapFuture(CompletableFuture<T> f) {
        return new RpcCallback<T>() {
            @Override
            public void success(ReadPacket<T> result) {
                f.complete(result.body);
            }

            @Override
            public void fail(Throwable ex) {
                f.completeExceptionally(ex);
            }
        };
    }

    static <T> RpcCallback<T> fromFuture(CompletableFuture<ReadPacket<T>> f) {
        return new RpcCallback<T>() {
            @Override
            public void success(ReadPacket<T> result) {
                f.complete(result);
            }

            @Override
            public void fail(Throwable ex) {
                f.completeExceptionally(ex);
            }
        };
    }

    static <T> RpcCallback<T> fromHandler(BiConsumer<ReadPacket<T>, Throwable> handler) {
        return new RpcCallback<T>() {
            @Override
            public void success(ReadPacket<T> result) {
                handler.accept(result, null);
            }

            @Override
            public void fail(Throwable ex) {
                handler.accept(null, ex);
            }
        };
    }

    static <T> RpcCallback<T> fromHandlerAsync(Executor executor, BiConsumer<ReadPacket<T>, Throwable> handler) {
        return new RpcCallback<T>() {
            @Override
            public void success(ReadPacket<T> result) {
                executor.execute(() -> handler.accept(result, null));
            }

            @Override
            public void fail(Throwable ex) {
                executor.execute(() -> handler.accept(null, ex));
            }
        };
    }
}

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
package com.github.dtprj.dongting.common;

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
@FunctionalInterface
public interface FutureCallback<T> {

    DtLog log = DtLogs.getLogger(FutureCallback.class);

    void call(T result, Throwable ex);

    static void callFail(FutureCallback<?> callback, Throwable ex) {
        try {
            if (callback != null) {
                callback.call(null, ex);
            }
        } catch (Throwable ex2) {
            log.error("FutureCallback error", ex2);
        }
    }

    static <T> void callSuccess(FutureCallback<T> callback, T result) {
        try {
            if (callback != null) {
                callback.call(result, null);
            }
        } catch (Throwable ex) {
            log.error("FutureCallback error", ex);
        }
    }

    static <T> FutureCallback<T> fromFuture(CompletableFuture<T> f) {
        return (result, ex) -> {
            if (ex != null) {
                f.completeExceptionally(ex);
            } else {
                f.complete(result);
            }
        };
    }
}

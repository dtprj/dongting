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

import com.github.dtprj.dongting.log.BugLog;

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
@SuppressWarnings("Convert2Diamond")
public interface FutureCallback<T> {

    void success(T result);

    void fail(Throwable ex);

    static void callFail(FutureCallback<?> callback, Throwable ex) {
        try {
            if (callback != null) {
                callback.fail(ex);
            }
        } catch (Throwable ex2) {
            BugLog.getLog().error("RpcCallback error", ex2);
        }
    }

    static <T> void callSuccess(FutureCallback<T> callback, T result) {
        try {
            if (callback != null) {
                callback.success(result);
            }
        } catch (Throwable ex) {
            BugLog.getLog().error("RpcCallback error", ex);
        }
    }

    static <T> FutureCallback<T> fromFuture(CompletableFuture<T> f) {
        return new FutureCallback<T>() {
            @Override
            public void success(T result) {
                f.complete(result);
            }

            @Override
            public void fail(Throwable ex) {
                f.completeExceptionally(ex);
            }
        };
    }
}

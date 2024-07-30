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

import com.github.dtprj.dongting.log.BugLog;

import java.util.concurrent.CompletableFuture;

/**
 * @author huangli
 */
@SuppressWarnings("Convert2Diamond")
public interface RpcCallback<T> {

    void success(ReadFrame<T> resp);

    void fail(Throwable ex);

    static <T> void callSuccess(RpcCallback<T> callback, ReadFrame<T> resp) {
        try {
            if (callback != null) {
                callback.success(resp);
            }
        } catch (Throwable ex) {
            BugLog.getLog().error("RpcCallback error", ex);
        }
    }

    static void callFail(RpcCallback<?> callback, Throwable ex) {
        try {
            if (callback != null) {
                callback.fail(ex);
            }
        } catch (Throwable ex2) {
            BugLog.getLog().error("RpcCallback error", ex2);
        }
    }

    static <T> RpcCallback<T> create(CompletableFuture<T> f) {
        return new RpcCallback<T>() {
            @Override
            public void success(ReadFrame<T> resp) {
                f.complete(resp.getBody());
            }

            @Override
            public void fail(Throwable ex) {
                f.completeExceptionally(ex);
            }
        };
    }
}

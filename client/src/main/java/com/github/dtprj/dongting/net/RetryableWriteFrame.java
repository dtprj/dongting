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

/**
 * @author huangli
 */
public abstract class RetryableWriteFrame extends WriteFrame {

    @Override
    protected final void doClean() {
        // WriteFrame.clean()/doClean() will be called after it's been written to the channel.
        // In retryable write frame, it may be called multiple times.
        // Since doClean() implementation may not be idempotent, we disable it here, user should
        // perform any cleanup in the callback of RPC.
    }

    public void reset() {
        super.reset();
    }
}

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
package com.github.dtprj.dongting.codec;

/**
 * @author huangli
 */
public final class PbLongCallback extends PbCallback<Long> {

    public static DecoderCallbackCreator<Long> CALLBACK_CREATOR =
            ctx -> ctx.toDecoderCallback(new PbLongCallback());

    @Override
    public boolean readFix64(int index, long value) {
        if (index == 1) {
            this.context.status = value;
        }
        return true;
    }

    @Override
    protected Long getResult() {
        Long r = (Long) this.context.status;
        return r == null ? 0 : r;
    }
}

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
package com.github.dtprj.dongting.fiber;

import com.github.dtprj.dongting.common.Pair;

/**
 * @author huangli
 */
public class HandlerFrame<O> extends FiberFrame<Pair<O, Throwable>> {

    private final FiberFrame<O> subFrame;

    public HandlerFrame(FiberFrame<O> subFrame) {
        this.subFrame = subFrame;
    }

    @Override
    public FrameCallResult execute(Void input) throws Exception {
        return Fiber.call(subFrame, this::resume);
    }

    private FrameCallResult resume(O o) {
        setResult(new Pair<>(o, null));
        return Fiber.frameReturn();
    }

    @Override
    protected FrameCallResult handle(Throwable ex) throws Throwable {
        setResult(new Pair<>(null, ex));
        return Fiber.frameReturn();
    }
}

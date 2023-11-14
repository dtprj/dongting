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

/**
 * @author huangli
 */
@SuppressWarnings("rawtypes")
public class FiberFrame<I, O> {
    FiberFrame prev;

    FrameCall<I, O> body;
    FrameCall<Throwable, O> catchClause;
    FrameCall<Void, O> finallyClause;
    FrameCall resumePoint;

    O result;

    FiberFrame() {
    }

    public static <I, O> FiberFrame<I, O> create(FrameCall<I, O> body,
                                                 FrameCall<Throwable, O> catchClause,
                                                 FrameCall<Void, O> finallyClause) {
        FiberFrame<I, O> frame = new FiberFrame<>();
        frame.body = body;
        frame.catchClause = catchClause;
        frame.finallyClause = finallyClause;
        return frame;
    }

    public static <I, O> FiberFrame<I, O> create(FrameCall<I, O> body) {
        return create(body, null, null);
    }

    public <O2> FiberFrame<I, O2> then(FrameCall<O, O2> nextAction) {
         FrameCall<I, O2> body = (currentFrame, input) ->
                 currentFrame.suspendCall(input, FiberFrame.this, nextAction);
        return create(body);
    }
}

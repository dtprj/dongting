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
package com.github.dtprj.dongting.bench.fiber;

import com.github.dtprj.dongting.bench.BenchBase;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.fiber.Dispatcher;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberChannel;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class ChannelCrossThreadTest extends BenchBase {

    private final Dispatcher dispatcher = new Dispatcher("testDispatcher");
    private final FiberGroup group = new FiberGroup("testGroup", dispatcher);
    private final FiberChannel<Object> channel = group.newChannel();

    public static void main(String[] args) throws Exception {
        new ChannelCrossThreadTest(1, 1000, 500).start();
    }

    public ChannelCrossThreadTest(int threadCount, long testTime, long warmupTime) {
        super(threadCount, testTime, warmupTime);
    }

    @Override
    public void init() {
        dispatcher.start();
        dispatcher.startGroup(group).join();
        group.fireFiber("consumer", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return channel.take(this::resume);
            }

            private FrameCallResult resume(Object o) {
                int s = state.getOpaque();
                if (s <= STATE_TEST) {
                    success(s);
                    return Fiber.resume(null, this);
                } else {
                    return Fiber.frameReturn();
                }
            }
        });

    }

    @Override
    public void shutdown() {
        dispatcher.stop(new DtTime(3, TimeUnit.SECONDS));
    }

    @Override
    public void test(int threadIndex, long startTime, int state) {
        channel.fireOffer(this);
    }
}

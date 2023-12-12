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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author huangli
 */
public class WaitSourceTest {
    private FiberFuture<Void> future;
    private Fiber f1;
    private Fiber f2;
    private Fiber f3;
    private Fiber f4;

    @BeforeEach
    public void setUp() {
        Dispatcher dispatcher = new Dispatcher("name");
        FiberGroup group = new FiberGroup("g", dispatcher);
        future = group.newFuture();
        f1 = new Fiber("f1", group, new EmptyFiberFrame());
        f2 = new Fiber("f2", group, new EmptyFiberFrame());
        f3 = new Fiber("f3", group, new EmptyFiberFrame());
        f4 = new Fiber("f4", group, new EmptyFiberFrame());
    }

    @Test
    public void testPopHead() {
        future.addWaiter(f1);
        future.addWaiter(f2);
        future.addWaiter(f3);
        Assertions.assertSame(f1, future.popHeadWaiter());
        Assertions.assertSame(f2, future.popHeadWaiter());
        Assertions.assertSame(f3, future.popHeadWaiter());
    }

    @Test
    public void testPopTail(){
        future.addWaiter(f1);
        future.addWaiter(f2);
        future.addWaiter(f3);
        Assertions.assertSame(f3, future.popTailWaiter());
        Assertions.assertSame(f2, future.popTailWaiter());
        Assertions.assertSame(f1, future.popTailWaiter());
    }

    @Test
    public void testRemove() {
        future.addWaiter(f1);
        future.addWaiter(f2);
        future.addWaiter(f3);
        future.addWaiter(f4);
        future.removeWaiter(f2);//mid
        future.removeWaiter(f1);//head
        future.removeWaiter(f4);//tail
        future.removeWaiter(f3);//last one
        Assertions.assertNull(future.popHeadWaiter());
        Assertions.assertNull(future.popTailWaiter());
    }
}

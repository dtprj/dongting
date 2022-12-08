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
package com.github.dtprj.dongting.common.j11;

import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.RefCount;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huangli
 */
public abstract class AbstractRefCountTest {

    private RefCount rf;

    @BeforeEach
    public void setup() {
        rf = createInstance();
    }

    protected abstract RefCount createInstance();

    @Test
    public void simpleTest() {
        assertTrue(rf.release());
        assertThrows(DtException.class, () -> rf.release());
    }

    @Test
    public void simpleTest2() {
        rf.retain();
        assertFalse(rf.release());
        assertTrue(rf.release());
        assertThrows(DtException.class, () -> rf.release());
        assertThrows(DtException.class, () -> rf.retain());
    }

    @Test
    public void simpleTest3() {
        rf.retain(5);
        assertFalse(rf.release());
        assertFalse(rf.release(2));
        assertTrue(rf.release(3));
        assertThrows(DtException.class, () -> rf.release());
        assertThrows(DtException.class, () -> rf.retain());
    }

    @Test
    public void illegalParamTest() {
        assertThrows(IllegalArgumentException.class, () -> rf.retain(0));
        assertThrows(IllegalArgumentException.class, () -> rf.release(0));
    }

    protected void doOverflowTest() {
        rf.retain();
        assertThrows(DtException.class, () -> rf.retain(Integer.MAX_VALUE / 2));
        assertFalse(rf.release());
        assertTrue(rf.release());
    }
}

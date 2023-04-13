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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
public class JavaVersionTest {
    @Test
    public void testMajorVersion() {
        assertEquals(8, DtUtil.majorVersion("1.8"));
        assertEquals(8, DtUtil.majorVersion("8"));
        assertEquals(9, DtUtil.majorVersion("1.9")); // early version of JDK 9 before Project Verona
        assertEquals(9, DtUtil.majorVersion("9"));
    }
}

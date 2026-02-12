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
package com.github.dtprj.dongting.log;

import com.github.dtprj.dongting.common.DtBugException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class BugLogTest {

    @BeforeEach
    void setUp() {
        BugLog.reset();
    }

    @AfterEach
    void tearDown() {
        BugLog.reset();
    }

    @Test
    void testLogFormatWithThrowable() {
        Exception ex = new NullPointerException("npe");
        BugLog.log("error code {}", 500, ex);

        assertEquals(1, BugLog.getCount());
        String firstError = BugLog.getFirstError();
        assertTrue(firstError.contains("error code 500"));
        assertTrue(firstError.contains("NullPointerException"));
        assertTrue(firstError.contains("npe"));
    }

    @Test
    void testLogFormatNullArg() {
        BugLog.log("value is {}", (Object) null);

        assertEquals(1, BugLog.getCount());
        assertTrue(BugLog.getFirstError().contains("value is null"));
    }

    @Test
    void testLogFormatNullArgs() {
        BugLog.log("{}, {}", null, null);

        assertEquals(1, BugLog.getCount());
        assertTrue(BugLog.getFirstError().contains("null, null"));
    }

    @Test
    void testLogFormatEmptyArgs() {
        BugLog.log("no substitution", new Object[0]);

        assertEquals(1, BugLog.getCount());
        assertTrue(BugLog.getFirstError().contains("no substitution"));
    }

    @Test
    void testLogFormatMorePlaceholdersThanArgs() {
        BugLog.log("{} and {} and {}", "a");

        assertTrue(BugLog.getFirstError().contains("a and {} and {}"));
    }

    @Test
    void testLogFormatMoreArgsThanPlaceholders() {
        BugLog.log("only {}", 1, 2, 3);

        assertTrue(BugLog.getFirstError().contains("only 1"));
    }

    @Test
    void testLogFormatWithThrowableAsLastArg() {
        Exception ex = new RuntimeException("ex");
        BugLog.log("{} {}", "a", ex);

        String firstError = BugLog.getFirstError();
        assertTrue(firstError.contains("a java.lang.RuntimeException: ex"));
    }

    @Test
    void testLogFormatWithThrowableAndNoPlaceholderForIt() {
        Exception ex = new IllegalStateException("ise");
        BugLog.log("context: {}", "test", ex);

        String firstError = BugLog.getFirstError();
        assertTrue(firstError.contains("context: test"));
        assertTrue(firstError.contains("IllegalStateException"));
    }

    @Test
    void testFirstErrorOnlySetOnce() {
        BugLog.log("first error");
        String first = BugLog.getFirstError();

        BugLog.log("second error");

        assertEquals(first, BugLog.getFirstError());
        assertTrue(first.contains("first error"));
        assertFalse(first.contains("second error"));
    }

    @Test
    void testLogAndThrow() {
        assertThrows(DtBugException.class, () -> BugLog.logAndThrow("should throw"));
        assertEquals(1, BugLog.getCount());
    }

    @Test
    void testCountIncrements() {
        BugLog.log("first");
        BugLog.log("second {}", 2);
        BugLog.log(new Exception());
        BugLog.log("msg", new Exception());
        assertEquals(4, BugLog.getCount());
    }
}

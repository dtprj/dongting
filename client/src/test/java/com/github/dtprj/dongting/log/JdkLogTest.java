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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author huangli
 */
public class JdkLogTest {
    @Test
    public void test() {
        Logger jdkLogger = Logger.getLogger(JdkLogTest.class.getName());
        jdkLogger.setLevel(Level.ALL);
        DtLog log = new JdkLog(jdkLogger);

        testLog(log);
    }

    static void testLog(DtLog log) {
        Assertions.assertTrue(log.isDebugEnabled());
        log.debug("msg");
        log.debug("msg {}", 1);
        log.debug("msg {} {}", 1, 2);
        log.debug("msg {} {} {}", 1, 2, 3);
        log.debug("msg {} {}", new Exception());

        Assertions.assertTrue(log.isInfoEnabled());
        log.info("msg");
        log.info("msg {}", 1);
        log.info("msg {} {}", 1, 2);
        log.info("msg {} {} {}", 1, 2, 3);
        log.info("msg {} {}", new Exception());

        Assertions.assertTrue(log.isWarnEnabled());
        log.warn("msg");
        log.warn("msg {}", 1);
        log.warn("msg {} {}", 1, 2);
        log.warn("msg {} {} {}", 1, 2, 3);
        log.warn("msg {} {}", new Exception());

        Assertions.assertTrue(log.isErrorEnabled());
        log.error("msg");
        log.error("msg {}", 1);
        log.error("msg {} {}", 1, 2);
        log.error("msg {} {} {}", 1, 2, 3);
        log.error("msg {} {}", new Exception());

        // bad params
        log.info("{} {}", 1);
        log.info("{}", 1, 2);

        // primitive types
        log.info("{} {}", new byte[]{1, 2});
        log.info("{} {}", new short[]{1, 2});
        log.info("{} {}", new char[]{1, 2});
        log.info("{} {}", new int[]{1, 2});
        log.info("{} {}", new long[]{1, 2});
        log.info("{} {}", new double[]{1, 2});
        log.info("{} {}", new float[]{1, 2});
        log.info("{} {}", new boolean[]{true, false});

        log.info("{} {}", new Byte[]{1, 2});
        log.info("{} {}", new Short[]{1, 2});
        log.info("{} {}", new Character[]{1, 2});
        log.info("{} {}", new Integer[]{1, 2});
        log.info("{} {}", new Long[]{1L, 2L});
        log.info("{} {}", new Double[]{1d, 2d});
        log.info("{} {}", new Float[]{1f, 2f});
        log.info("{} {}", new Boolean[]{true, false});

        log.info("{} {}", new BigDecimal[]{new BigDecimal(1), new BigDecimal(2)});
    }
}

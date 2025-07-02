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
package com.github.dtprj.dongting.raft.test;

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.common.AbstractLifeCircle;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
@SuppressWarnings("unused")
public class TestUtil {
    private static final DtLog log = DtLogs.getLogger(TestUtil.class);

    public static void updateTimestamp(Timestamp ts, long nanoTime, long wallClockMillis) {
        ts.nanoTime = nanoTime;
        ts.wallClockMillis = wallClockMillis;
    }

    public static void plus1Hour(Timestamp ts) {
        ts.nanoTime += TimeUnit.HOURS.toNanos(1);
        ts.wallClockMillis += TimeUnit.HOURS.toMillis(1);
    }

    public static void plus(Timestamp ts, int value, TimeUnit unit) {
        ts.nanoTime += unit.toNanos(value);
        ts.wallClockMillis += unit.toMillis(value);
    }

    public static RefBufferFactory heapPool() {
        ByteBufferPool p = new DefaultPoolFactory().createPool(new Timestamp(), false);
        return new RefBufferFactory(p, 0);
    }

    public static ByteBufferPool directPool() {
        return new DefaultPoolFactory().createPool(new Timestamp(), true);
    }

    public static String randomStr(int length) {
        Random r = new Random();
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (r.nextInt(26) + 'a');
        }
        return new String(bytes);
    }

    public static void stop(AbstractLifeCircle obj) {
        if (obj == null) {
            return;
        }
        try {
            obj.stop(new DtTime(5, TimeUnit.SECONDS), true);
        } catch (Exception e) {
            log.error("", e);
        }
    }
}

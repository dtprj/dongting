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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.queue.MpscLinkedQueue;

/**
 * @author huangli
 */
public abstract class VersionFactory {

    public static VersionFactory getInstance() {
        return VfHolder.FACTORY;
    }

    public abstract AbstractRefCountUpdater newRefCountUpdater(boolean plain);

    public abstract <E> MpscLinkedQueue<E> newMpscLinkedQueue();

    public abstract void releaseFence();

    public abstract void acquireFence();

    public abstract void fullFence();
}

class VfHolder {
    private static final DtLog log = DtLogs.getLogger(RefCount.class);

    static final VersionFactory FACTORY;

    static {
        Class<?> java8Factory = null;
        try {
            java8Factory = Class.forName("com.github.dtprj.dongting.vf8.Java8Factory");
        } catch (ClassNotFoundException ignored) {
        }
        Class<?> java11Factory = null;
        try {
            java11Factory = Class.forName("com.github.dtprj.dongting.java11.Java11Factory");
        } catch (ClassNotFoundException ignored) {
        }
        if (java8Factory == null && java11Factory == null) {
            throw new DtException("can't find VersionFactory implementation");
        }
        Class<?> factoryClass = java8Factory;
        if (DtUtil.javaVersion() > 8 && java11Factory != null) {
            factoryClass = java11Factory;
        }
        VersionFactory f = null;
        try {
            f = (VersionFactory) factoryClass.getDeclaredConstructor().newInstance();
        } catch (Throwable e) {
            log.error("can't init VersionFactory instance", e);
            throw new DtException(e);
        } finally {
            FACTORY = f;
        }

    }
}

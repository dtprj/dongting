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

import com.github.dtprj.dongting.java8.Java8Factory;
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
}

class VfHolder {
    private static final DtLog log = DtLogs.getLogger(RefCount.class);

    static final VersionFactory FACTORY;

    static {
        if (JavaVersion.javaVersion() < 11) {
            FACTORY = new Java8Factory();
        } else {
            VersionFactory f = null;
            try {
                Class<?> clz = Class.forName("com.github.dtprj.dongting.java11.Java11Factory");
                f = (VersionFactory) clz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                log.error("", e);
            }
            FACTORY = f != null ? f : new Java8Factory();
        }
    }
}

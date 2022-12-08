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

/**
 * @author huangli
 * <p>
 * see netty ReferenceCountUpdater.
 */
public abstract class RefCount {

    private static final DtLog log = DtLogs.getLogger(RefCount.class);

    private static final RefCountFactory FACTORY;

    static {
        if (JavaVersion.javaVersion() < 11) {
            FACTORY = new Java8RefCount.Java8RefCountFactory();
        } else {
            RefCountFactory f = null;
            try {
                Class<?> clz = Class.forName("com.github.dtprj.dongting.common.j11.VarHandleRefCount$VarHandleRefCountFactory");
                f = (RefCountFactory) clz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                log.error("", e);
            }
            FACTORY = f != null ? f : new Java8RefCount.Java8RefCountFactory();
        }
    }

    protected RefCount() {
    }

    public static RefCount newInstance() {
        return FACTORY.newInstance();
    }

    /**
     * return RefCount instance that is not threadSafe
     */
    public static RefCount newPlainInstance() {
        return new PlainRefCount();
    }

    public abstract void retain();

    public abstract void retain(int increment);

    public abstract boolean release();

    public abstract boolean release(int decrement);

    public static abstract class RefCountFactory {
        public abstract RefCount newInstance();
    }

}
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
package com.github.dtprj.dongting.java8;

import com.github.dtprj.dongting.common.AbstractRefCountUpdater;
import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.VersionFactory;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.queue.MpscLinkedQueue;
import com.github.dtprj.dongting.unsafe.DtUnsafe;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * @author huangli
 */
@SuppressWarnings("unused")
public class Java8Factory extends VersionFactory {

    private static final DtLog log = DtLogs.getLogger(Java8Factory.class);

    private static final Method GET_CLEANER_METHOD;
    private static final Method CLEAN_METHOD;

    static {
        Method getCleanerMethod = null;
        Method cleanMethod = null;
        try {
            ByteBuffer directBuffer = ByteBuffer.allocateDirect(1);
            getCleanerMethod = directBuffer.getClass().getMethod("cleaner");
            getCleanerMethod.setAccessible(true);
            Object cleaner = getCleanerMethod.invoke(directBuffer);
            cleanMethod = cleaner.getClass().getMethod("clean");
            cleanMethod.invoke(cleaner);
        } catch (Throwable e) {
            log.error("can't init direct buffer cleaner", e);
            getCleanerMethod = null;
            cleanMethod = null;
        } finally {
            GET_CLEANER_METHOD = getCleanerMethod;
            CLEAN_METHOD = cleanMethod;
        }
    }

    @Override
    public AbstractRefCountUpdater newRefCountUpdater(boolean plain) {
        return plain ? PlainRefCountUpdater.getInstance() : Java8RefCountUpdater.getInstance();
    }

    @Override
    public <E> MpscLinkedQueue<E> newMpscLinkedQueue() {
        return new Java8MpscLinkedQueue<>();
    }

    @Override
    public void releaseDirectBuffer(ByteBuffer buffer) {
        if (!buffer.isDirect()) {
            throw new DtException("not direct buffer");
        }
        if (GET_CLEANER_METHOD != null) {
            try {
                Object cleaner = GET_CLEANER_METHOD.invoke(buffer);
                CLEAN_METHOD.invoke(cleaner);
            } catch (Exception e) {
                log.error("can't clean direct buffer", e);
            }
        }
    }

    @Override
    public void releaseFence() {
        DtUnsafe.releaseFence();
    }

    @Override
    public void acquireFence() {
        DtUnsafe.acquireFence();
    }

    @Override
    public void fullFence() {
        DtUnsafe.fullFence();
    }
}

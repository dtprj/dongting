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
package com.github.dtprj.dongting.java11;

import com.github.dtprj.dongting.common.AbstractRefCountUpdater;
import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.ObjUtil;
import com.github.dtprj.dongting.common.RefCount;

import java.lang.invoke.VarHandle;

/**
 * @author huangli
 */
public class PlainRefCountUpdater extends AbstractRefCountUpdater {
    private static final VarHandle REF_CNT = VarHandleRefCount.REF_CNT;
    private static final PlainRefCountUpdater INSTANCE = new PlainRefCountUpdater();

    private PlainRefCountUpdater() {
    }

    public static PlainRefCountUpdater getInstance() {
        return INSTANCE;
    }

    @Override
    public void init(RefCount instance) {
        REF_CNT.set(instance, 1);
    }

    @Override
    public void retain(RefCount instance, int increment) {
        ObjUtil.checkPositive(increment, "increment");
        int refCnt = (int) REF_CNT.get(instance);
        if (refCnt <= 0) {
            throw new DtException("already released");
        }
        REF_CNT.set(instance, refCnt + increment);
    }

    @Override
    public boolean release(RefCount instance, int decrement) {
        ObjUtil.checkPositive(decrement, "decrement");
        int refCnt = (int) REF_CNT.get(instance);
        if (refCnt < decrement) {
            throw new DtException("decrement>refCnt," + decrement + "," + refCnt);
        }
        refCnt -= decrement;
        REF_CNT.set(instance, refCnt);
        return refCnt == 0;
    }

    @Override
    public boolean isReleased(RefCount instance) {
        return ((int) REF_CNT.get(instance)) <= 0;
    }
}

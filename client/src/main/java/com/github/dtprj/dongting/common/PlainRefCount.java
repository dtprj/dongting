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

/**
 * @author huangli
 */
public class PlainRefCount extends RefCount {

    private int refCnt;

    protected PlainRefCount() {
        this.refCnt = 1;
    }

    @Override
    public void retain() {
        retain(1);
    }

    @Override
    public void retain(int increment) {
        ObjUtil.checkPositive(increment, "increment");
        int refCnt = this.refCnt;
        if (refCnt <= 0) {
            throw new DtException("already released");
        }
        this.refCnt = refCnt + increment;
    }

    @Override
    public boolean release() {
        return release(1);
    }

    @Override
    public boolean release(int decrement) {
        ObjUtil.checkPositive(decrement, "decrement");
        int refCnt = this.refCnt;
        if (refCnt < decrement) {
            throw new DtException("decrement>refCnt," + decrement + "," + refCnt);
        }
        refCnt -= decrement;
        this.refCnt = refCnt;
        return refCnt == 0;
    }
}

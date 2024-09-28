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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvNode;

/**
 * @author huangli
 */
public class KvResult {

    private final int code;
    private KvNode data;

    public static final KvResult SUCCESS = new KvResult(KvCodes.CODE_SUCCESS);
    public static final KvResult NOT_FOUND = new KvResult(KvCodes.CODE_NOT_FOUND);
    public static final KvResult SUCCESS_OVERWRITE = new KvResult(KvCodes.CODE_SUCCESS_OVERWRITE);

    public KvResult(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public KvNode getData() {
        return data;
    }

    public void setData(KvNode data) {
        this.data = data;
    }
}

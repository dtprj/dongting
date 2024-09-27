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

/**
 * @author huangli
 */
public class KvResult {

    public static final int CODE_SUCCESS = 0;
    public static final int CODE_NOT_FOUND = 1;
    public static final int CODE_SUCCESS_OVERWRITE = 2;
    public static final int CODE_NOT_VALUE = 3;
    public static final int CODE_NOT_DIR = 4;
    public static final int CODE_KEY_IS_NULL = 5;
    public static final int CODE_VALUE_IS_NULL = 6;
    public static final int CODE_INVALID_KEY = 7;
    public static final int CODE_DIR_NOT_EXISTS = 8;
    public static final int CODE_HAS_CHILDREN = 9;

    private final int code;
    private KvNode data;

    public static final KvResult SUCCESS = new KvResult(CODE_SUCCESS);
    public static final KvResult NOT_FOUND = new KvResult(CODE_NOT_FOUND);
    public static final KvResult SUCCESS_OVERWRITE = new KvResult(CODE_SUCCESS_OVERWRITE);

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

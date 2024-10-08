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
package com.github.dtprj.dongting.dtkv;

/**
 * @author huangli
 */
public interface KvCodes {

    int CODE_SUCCESS = 0;
    int CODE_NOT_FOUND = 1;
    int CODE_SUCCESS_OVERWRITE = 2;
    int CODE_DIR_EXISTS = 3;
    int CODE_VALUE_EXISTS = 4;
    int CODE_PARENT_NOT_DIR = 5;
    int CODE_KEY_IS_NULL = 6;
    int CODE_VALUE_IS_NULL = 7;
    int CODE_INVALID_KEY = 8;
    int CODE_PARENT_DIR_NOT_EXISTS = 9;
    int CODE_HAS_CHILDREN = 10;
    int CODE_KEY_TOO_LONG = 11;
    int CODE_VALUE_TOO_LONG = 12;
}

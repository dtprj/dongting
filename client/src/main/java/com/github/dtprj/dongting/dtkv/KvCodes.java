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
    int CODE_INVALID_KEY = 6;
    int CODE_INVALID_VALUE = 7;
    int CODE_PARENT_DIR_NOT_EXISTS = 8;
    int CODE_HAS_CHILDREN = 9;
    int CODE_KEY_TOO_LONG = 10;
    int CODE_VALUE_TOO_LONG = 11;
    int CODE_INSTALL_SNAPSHOT = 12;
    int CODE_CAS_MISMATCH = 13;
    int CODE_CLIENT_REQ_ERROR = 14;
    int CODE_REMOVE_WATCH = 15;
    int CODE_REMOVE_ALL_WATCH = 16;
    int CODE_NOT_OWNER = 17;
    int CODE_NOT_TEMP_NODE = 18;
    int CODE_IS_TEMP_NODE = 19;
    int CODE_NOT_EXPIRED = 20;

    static String toStr(int code) {
        switch (code) {
            case CODE_SUCCESS:
                return "SUCCESS";
            case CODE_NOT_FOUND:
                return "NOT_FOUND";
            case CODE_SUCCESS_OVERWRITE:
                return "SUCCESS_OVERWRITE";
            case CODE_DIR_EXISTS:
                return "DIR_EXISTS";
            case CODE_VALUE_EXISTS:
                return "VALUE_EXISTS";
            case CODE_PARENT_NOT_DIR:
                return "PARENT_NOT_DIR";
            case CODE_INVALID_KEY:
                return "INVALID_KEY";
            case CODE_INVALID_VALUE:
                return "INVALID_VALUE";
            case CODE_PARENT_DIR_NOT_EXISTS:
                return "PARENT_DIR_NOT_EXISTS";
            case CODE_HAS_CHILDREN:
                return "HAS_CHILDREN";
            case CODE_KEY_TOO_LONG:
                return "KEY_TOO_LONG";
            case CODE_VALUE_TOO_LONG:
                return "VALUE_TOO_LONG";
            case CODE_INSTALL_SNAPSHOT:
                return "INSTALL_SNAPSHOT";
            case CODE_CAS_MISMATCH:
                return "CAS_MISMATCH";
            case CODE_CLIENT_REQ_ERROR:
                return "CLIENT_REQ_ERROR";
            case CODE_REMOVE_WATCH:
                return "REMOVE_WATCH";
            case CODE_REMOVE_ALL_WATCH:
                return "REMOVE_ALL_WATCH";
            case CODE_NOT_OWNER:
                return "NOT_OWNER";
            case CODE_NOT_TEMP_NODE:
                return "NOT_TEMP_NODE";
            case CODE_IS_TEMP_NODE:
                return "IS_TEMP_NODE";
            case CODE_NOT_EXPIRED:
                return "NOT_EXPIRED";
            default:
                return "UNKNOWN_CODE_" + code;
        }
    }
}

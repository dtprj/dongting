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

    int SUCCESS = 0;
    int NOT_FOUND = 1;
    int SUCCESS_OVERWRITE = 2;
    int DIR_EXISTS = 3;
    int VALUE_EXISTS = 4;
    int PARENT_NOT_DIR = 5;
    int INVALID_KEY = 6;
    int INVALID_VALUE = 7;
    int PARENT_DIR_NOT_EXISTS = 8;
    int HAS_CHILDREN = 9;
    int KEY_TOO_LONG = 10;
    int VALUE_TOO_LONG = 11;
    int INSTALL_SNAPSHOT = 12;
    int CAS_MISMATCH = 13;
    int CLIENT_REQ_ERROR = 14;
    int REMOVE_WATCH = 15;
    int REMOVE_ALL_WATCH = 16;
    int NOT_OWNER = 17;
    int NOT_TEMP_NODE = 18;
    int IS_TEMP_NODE = 19;
    int NOT_EXPIRED = 20;
    int INVALID_TTL = 21;
    int TTL_INDEX_MISMATCH = 22;

    static String toStr(int code) {
        switch (code) {
            case SUCCESS:
                return "SUCCESS";
            case NOT_FOUND:
                return "NOT_FOUND";
            case SUCCESS_OVERWRITE:
                return "SUCCESS_OVERWRITE";
            case DIR_EXISTS:
                return "DIR_EXISTS";
            case VALUE_EXISTS:
                return "VALUE_EXISTS";
            case PARENT_NOT_DIR:
                return "PARENT_NOT_DIR";
            case INVALID_KEY:
                return "INVALID_KEY";
            case INVALID_VALUE:
                return "INVALID_VALUE";
            case PARENT_DIR_NOT_EXISTS:
                return "PARENT_DIR_NOT_EXISTS";
            case HAS_CHILDREN:
                return "HAS_CHILDREN";
            case KEY_TOO_LONG:
                return "KEY_TOO_LONG";
            case VALUE_TOO_LONG:
                return "VALUE_TOO_LONG";
            case INSTALL_SNAPSHOT:
                return "INSTALL_SNAPSHOT";
            case CAS_MISMATCH:
                return "CAS_MISMATCH";
            case CLIENT_REQ_ERROR:
                return "CLIENT_REQ_ERROR";
            case REMOVE_WATCH:
                return "REMOVE_WATCH";
            case REMOVE_ALL_WATCH:
                return "REMOVE_ALL_WATCH";
            case NOT_OWNER:
                return "NOT_OWNER";
            case NOT_TEMP_NODE:
                return "NOT_TEMP_NODE";
            case IS_TEMP_NODE:
                return "IS_TEMP_NODE";
            case NOT_EXPIRED:
                return "NOT_EXPIRED";
            case INVALID_TTL:
                return "INVALID_TTL";
            case TTL_INDEX_MISMATCH:
                return "TTL_INDEX_MISMATCH";
            default:
                return "UNKNOWN_CODE_" + code;
        }
    }
}

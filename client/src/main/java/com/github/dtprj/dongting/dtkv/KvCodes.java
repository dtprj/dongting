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
    int ERROR = 1;
    int NOT_FOUND = 2;
    int SUCCESS_OVERWRITE = 3;
    int DIR_EXISTS = 4;
    int VALUE_EXISTS = 5;
    int PARENT_NOT_DIR = 6;
    int INVALID_KEY = 7;
    int INVALID_VALUE = 8;
    int PARENT_DIR_NOT_EXISTS = 9;
    int HAS_CHILDREN = 10;
    int KEY_TOO_LONG = 11;
    int VALUE_TOO_LONG = 12;
    int INSTALL_SNAPSHOT = 13;
    int CAS_MISMATCH = 14;
    int CLIENT_REQ_ERROR = 15;
    int REMOVE_WATCH = 16;
    int REMOVE_ALL_WATCH = 17;
    int NOT_OWNER = 18;
    int NOT_TEMP_NODE = 19;
    int IS_TEMP_NODE = 20;
    int NOT_EXPIRED = 21;
    int INVALID_TTL = 22;
    int TTL_INDEX_MISMATCH = 23;
    int UNDER_TEMP_DIR = 24;
    int PARENT_IS_LOCK = 25;
    int NOT_LOCK_NODE = 26;
    int LOCK_BY_SELF = 27;
    int LOCK_BY_OTHER = 28;


    static String toStr(int code) {
        switch (code) {
            case SUCCESS:
                return "SUCCESS";
            case ERROR:
                return "ERROR";
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
            case UNDER_TEMP_DIR:
                return "UNDER_TEMP_DIR";
            case PARENT_IS_LOCK:
                return "PARENT_IS_LOCK";
            case NOT_LOCK_NODE:
                return "NOT_LOCK_NODE";
            case LOCK_BY_SELF:
                return "LOCK_BY_SELF";
            case LOCK_BY_OTHER:
                return "LOCK_BY_OTHER";
            default:
                return "UNKNOWN_CODE_" + code;
        }
    }
}

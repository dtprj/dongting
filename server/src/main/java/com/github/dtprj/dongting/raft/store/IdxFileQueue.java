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
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

/**
 * @author huangli
 */
class IdxFileQueue {
    private static final DtLog log = DtLogs.getLogger(IdxFileQueue.class);
    private static final int ITEM_LEN = 8;
    static final String KEY_PERSIST_IDX_INDEX = "persistIdxIndex";
    static final String KEY_NEXT_IDX_AFTER_INSTALL_SNAPSHOT = "nextIdxAfterInstallSnapshot";
    static final String KEY_NEXT_POS_AFTER_INSTALL_SNAPSHOT = "nextPosAfterInstallSnapshot";
    static final String KEY_INSTALL_SNAPSHOT = "installSnapshot";

    public static final int DEFAULT_ITEMS_PER_FILE = 1024 * 1024;
    public static final int DEFAULT_MAX_CACHE_ITEMS = 16 * 1024;
}

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

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.util.function.Supplier;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class Restorer {
    private static final DtLog log = DtLogs.getLogger(Restorer.class);

    private static final int STATE_ITEM_HEADER = 1;
    private static final int STATE_BIZ_HEADER = 2;
    private static final int STATE_BIZ_BODY = 3;

    private static final int RT_CONTINUE_LOAD = 1;
    private static final int RT_CONTINUE_READ = 2;
    private static final int RT_CURRENT_FILE_FINISHED = 3;
    private static final int RT_RESTORE_FINISHED = 4;


    private final CRC32C crc32c = new CRC32C();
    private final IdxOps idxOps;
    private final LogFileQueue logFileQueue;
    private final long restoreIndex;
    private final long restoreIndexPos;
    private final long firstValidPos;

    private boolean restoreIndexChecked;

    private long itemStartPosOfFile;
    private int state;
    private final LogHeader header = new LogHeader();
    private int dataReadLength;

    long previousIndex;
    int previousTerm;

    public Restorer(IdxOps idxOps, LogFileQueue logFileQueue, long restoreIndex,
                    long restoreIndexPos, long firstValidPos) {
        this.idxOps = idxOps;
        this.logFileQueue = logFileQueue;
        this.restoreIndex = restoreIndex;
        this.restoreIndexPos = restoreIndexPos;
        this.firstValidPos = firstValidPos;
    }

    public FiberFrame<Pair<Boolean, Long>> restoreFile(ByteBuffer writeBuffer, LogFile lf,
                                                       Supplier<Boolean> stopIndicator) {
    }
}

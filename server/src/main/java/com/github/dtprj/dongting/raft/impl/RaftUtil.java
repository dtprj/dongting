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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class RaftUtil {
    private static final DtLog log = DtLogs.getLogger(RaftUtil.class);

    public static void updateCrc(CRC32C crc32c, ByteBuffer buf, int startPos, int len) {
        if (len == 0) {
            return;
        }
        int oldPos = buf.position();
        int oldLimit = buf.limit();
        buf.limit(startPos + len);
        buf.position(startPos);
        crc32c.update(buf);
        buf.limit(oldLimit);
        buf.position(oldPos);
    }

    public static void checkStop(FiberGroup fiberGroup) {
        if (fiberGroup.isShouldStop()) {
            throw new RaftException("raft group stopped");
        }
    }

}

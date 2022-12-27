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

import com.github.dtprj.dongting.common.LifeCircle;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.net.Socket;

/**
 * @author huangli
 */
public class CloseUtil {
    private static final DtLog log = DtLogs.getLogger(CloseUtil.class);

    public static void close(Object... resources) {
        for (Object res : resources) {
            if (res != null) {
                try {
                    if (res instanceof AutoCloseable) {
                        ((AutoCloseable) res).close();
                    } else if (res instanceof LifeCircle) {
                        ((LifeCircle) res).stop();
                    } else if (res instanceof Socket) {
                        ((Socket) res).close();
                    }
                } catch (Throwable e) {
                    log.error("close fail", e);
                }
            }
        }
    }
}

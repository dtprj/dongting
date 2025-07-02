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
package com.github.dtprj.dongting.common;

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class TestUtil {
    private static final DtLog log = DtLogs.getLogger(TestUtil.class);

    public static void stop(LifeCircle... lifeCircle) {
        for (LifeCircle lc : lifeCircle) {
            try {
                if (lc != null) {
                    lc.stop(new DtTime(5, TimeUnit.SECONDS));
                }
            } catch (Exception e) {
                log.error("stop fail", e);
            }
        }
    }
}

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
package com.github.dtprj.dongting.common.log;

public class DtLogs {
    private static DtLogFactory instance;

    static {
        if (Slf4jFactory.slf4jExists()) {
            instance = new Slf4jFactory();
        } else {
            instance = new JdkFactory();
        }
    }

    public static DtLogFactory getInstance() {
        return instance;
    }

    public static void setInstance(DtLogFactory instance) {
        DtLogs.instance = instance;
    }
}

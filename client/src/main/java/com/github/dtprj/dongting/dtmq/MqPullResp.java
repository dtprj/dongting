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
package com.github.dtprj.dongting.dtmq;

import java.util.Collections;
import java.util.List;

/**
 * @author huangli
 */
public class MqPullResp {
    public static final int IDX_RESULTS_SIZE = 1;
    public static final int IDX_RESULTS = 2;
    public static final int IDX_RECORDS = 15;

    // same order as requested queues
    public List<MqPullRespDetail> results = Collections.emptyList();
    // grouped by results, in the same order
    public List<DtMsg> records = Collections.emptyList();
}

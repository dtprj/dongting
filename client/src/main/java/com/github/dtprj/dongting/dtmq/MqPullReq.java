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

/**
 * @author huangli
 */
public class MqPullReq {
    public static final int IDX_GROUP_ID = 1;
    public static final int IDX_QUEUES_SIZE = 2;
    public static final int IDX_LIMIT = 3;
    public static final int IDX_BYTES_LIMIT = 4;
    public static final int IDX_WAIT_MILLIS = 5;
    public static final int IDX_QUEUES = 15;

    // MqPullReqDetail
    public static final int IDX_DETAIL_QUEUE_ID = 1;
    public static final int IDX_DETAIL_FROM_SEQ = 2;

    public int groupId;
    // 1-based queue ids; 0 is reserved
    public long[] queueIds;
    // 0 starts at the earliest readable record
    public long[] fromSeqs;
    // shared record limit hint across all queues
    public int limit;
    // shared raw log byte hint (no metadata); any single record may cross it, collection stops once reached
    public int bytesLimit;
    // 0 disables long polling
    public long waitMillis;
}

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
public class DtMsg {
    // 1-based; 0 is reserved
    public long queueId;
    public byte[] header;
    public byte[] body;
    public long timestamp;
    // 0-based, assigned by the server when received
    public long seq;

    public DtMsg(long queueId, byte[] header, byte[] body) {
        this.queueId = queueId;
        this.header = header;
        this.body = body;
    }

    public DtMsg(long queueId, byte[] header, byte[] body, long timestamp, long seq) {
        this.queueId = queueId;
        this.header = header;
        this.body = body;
        this.timestamp = timestamp;
        this.seq = seq;
    }
}

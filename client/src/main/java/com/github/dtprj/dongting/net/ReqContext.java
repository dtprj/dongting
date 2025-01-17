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
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.common.DtTime;

/**
 * @author huangli
 */
public class ReqContext {
    private final DtChannelImpl dtChannel;
    private final DtTime timeout;
    private final RespWriter respWriter;

    ReqContext(DtChannelImpl dtChannel, RespWriter respWriter, DtTime timeout) {
        this.dtChannel = dtChannel;
        this.respWriter = respWriter;
        this.timeout = timeout;
    }

    public DtTime getTimeout() {
        return timeout;
    }

    public DtChannel getDtChannel() {
        return dtChannel;
    }

    public RespWriter getRespWriter() {
        return respWriter;
    }
}

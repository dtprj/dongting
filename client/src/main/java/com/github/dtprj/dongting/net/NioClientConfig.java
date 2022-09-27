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

import java.util.List;

/**
 * @author huangli
 */
public class NioClientConfig extends NioConfig {
    private List<HostPort> hostPorts;
    private int waitStartTimeoutMillis = 2000;

    public NioClientConfig() {
        setName("DtNioClient");
        setBizThreads(0);
        setBizQueueSize(500);

        // TODO negotiate with server
        setMaxOutRequests(2000);
        setMaxInRequests(100);
        setMaxInBytes(32 * 1024 * 1024);
    }

    public List<HostPort> getHostPorts() {
        return hostPorts;
    }

    public void setHostPorts(List<HostPort> hostPorts) {
        this.hostPorts = hostPorts;
    }

    public int getWaitStartTimeoutMillis() {
        return waitStartTimeoutMillis;
    }

    public void setWaitStartTimeoutMillis(int waitStartTimeoutMillis) {
        this.waitStartTimeoutMillis = waitStartTimeoutMillis;
    }
}

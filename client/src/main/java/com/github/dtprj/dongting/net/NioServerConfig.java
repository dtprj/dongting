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

/**
 * @author huangli
 */
public class NioServerConfig extends NioConfig {
    public int port;
    public int[] ports;
    public int ioThreads;
    public int backlog = 4096;

    public NioServerConfig() {
        this.ioThreads = calcIoThreads();
        this.bizThreads = Runtime.getRuntime().availableProcessors() * 2;
        if (bizThreads < 6) {
            this.bizThreads = 6;
        }
        if (bizThreads > 16) {
            this.bizThreads = 16;
        }
        this.name = "DtNioServer";

        // back pressure config
        this.maxOutRequests = 0;
        this.maxOutBytes = 0;
        this.maxInRequests = 100_000;
        this.maxInBytes = 512 * 1024 * 1024;
    }

    private int calcIoThreads() {
        int p = Runtime.getRuntime().availableProcessors();
        if (p <= 3) {
            return 1;
        } else if (p <= 6) {
            return 2;
        } else if (p <= 12) {
            return 3;
        } else if (p <= 24) {
            return 4;
        } else if (p <= 40) {
            return 5;
        } else if (p <= 64) {
            return 6;
        } else if (p <= 100) {
            return 7;
        } else {
            return 8;
        }
    }

}

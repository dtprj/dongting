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
package com.github.dtprj.dongting.remoting;

/**
 * @author huangli
 */
public class NioServerConfig extends NioConfig {
    private int port;
    private int ioThreads;

    public NioServerConfig() {
        setIoThreads(calcIoThreads());
        int bizThreads = Runtime.getRuntime().availableProcessors() * 4;
        setBizThreads(bizThreads);
        setBizQueueSize(5000);
        setName("DtNioServer");

        // back pressure config
        setMaxOutRequests(10000);
        setMaxInRequests(200_000);
        setMaxInBytes(512 * 1024 * 1024);
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


    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getIoThreads() {
        return ioThreads;
    }

    public void setIoThreads(int ioThreads) {
        this.ioThreads = ioThreads;
    }
}

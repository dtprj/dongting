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

public class NioServerConfig {
    private int ioThreads = computeIoThreads(Runtime.getRuntime().availableProcessors());
    private int bizThreads = Runtime.getRuntime().availableProcessors() * 4;
    private int port;
    private String name = "DtNioServer";

    public NioServerConfig() {
    }

    public static int computeIoThreads(int processorCount) {
        return computeThreads(processorCount, 0, 0.4);
    }

    private static int computeThreads(int processorCount, int delta, double pow) {
        return Math.min(((int) Math.pow(processorCount + delta, pow)) + 1, processorCount);
    }

    public int getIoThreads() {
        return ioThreads;
    }

    public void setIoThreads(int ioThreads) {
        this.ioThreads = ioThreads;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getBizThreads() {
        return bizThreads;
    }

    public void setBizThreads(int bizThreads) {
        this.bizThreads = bizThreads;
    }
}

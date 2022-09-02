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
public abstract class NioConfig {
    private int bizThreads;
    private int bizQueueSize = 5000;
    private String name;
    private int maxRequestPending = 2000;
    private int requestTimeoutMillis = 2000;
    private int selectTimeoutMillis = 50;
    private int cleanIntervalMills = 100;

    public int getBizThreads() {
        return bizThreads;
    }

    public void setBizThreads(int bizThreads) {
        this.bizThreads = bizThreads;
    }

    public int getBizQueueSize() {
        return bizQueueSize;
    }

    public void setBizQueueSize(int bizQueueSize) {
        this.bizQueueSize = bizQueueSize;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMaxRequestPending() {
        return maxRequestPending;
    }

    public void setMaxRequestPending(int maxRequestPending) {
        this.maxRequestPending = maxRequestPending;
    }

    public int getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public void setRequestTimeoutMillis(int requestTimeoutMillis) {
        this.requestTimeoutMillis = requestTimeoutMillis;
    }

    public int getSelectTimeoutMillis() {
        return selectTimeoutMillis;
    }

    public void setSelectTimeoutMillis(int selectTimeoutMillis) {
        this.selectTimeoutMillis = selectTimeoutMillis;
    }

    public int getCleanIntervalMills() {
        return cleanIntervalMills;
    }

    public void setCleanIntervalMills(int cleanIntervalMills) {
        this.cleanIntervalMills = cleanIntervalMills;
    }
}

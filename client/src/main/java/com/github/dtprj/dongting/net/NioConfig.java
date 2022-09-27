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
public abstract class NioConfig {
    private int bizThreads;
    private String name;

    // back pressure config
    private int maxOutRequests;
    private int maxInRequests;
    private long maxInBytes;

    private int selectTimeoutMillis = 50;
    private int cleanIntervalMills = 100;
    private int closeTimeoutMillis = 1000;

    private int maxFrameSize = 8 * 1024 * 1024;

    private int[] bufPoolSize = ByteBufferPool.DEFAULT_BUF_SIZE;
    private int[] bufPoolMinCount = ByteBufferPool.DEFAULT_MIN_COUNT;
    private int[] bufPoolMaxCount = ByteBufferPool.DEFAULT_MAX_COUNT;
    private long bufPoolTimeout = ByteBufferPool.DEFAULT_TIME_OUT_MILLIS;


    public int getBizThreads() {
        return bizThreads;
    }

    public void setBizThreads(int bizThreads) {
        this.bizThreads = bizThreads;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMaxOutRequests() {
        return maxOutRequests;
    }

    public void setMaxOutRequests(int maxOutRequests) {
        this.maxOutRequests = maxOutRequests;
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

    public int getCloseTimeoutMillis() {
        return closeTimeoutMillis;
    }

    public void setCloseTimeoutMillis(int closeTimeoutMillis) {
        this.closeTimeoutMillis = closeTimeoutMillis;
    }

    public int getMaxInRequests() {
        return maxInRequests;
    }

    public void setMaxInRequests(int maxInRequests) {
        this.maxInRequests = maxInRequests;
    }

    public long getMaxInBytes() {
        return maxInBytes;
    }

    public void setMaxInBytes(long maxInBytes) {
        this.maxInBytes = maxInBytes;
    }

    public int[] getBufPoolSize() {
        return bufPoolSize;
    }

    public void setBufPoolSize(int[] bufPoolSize) {
        this.bufPoolSize = bufPoolSize;
    }

    public int[] getBufPoolMinCount() {
        return bufPoolMinCount;
    }

    public void setBufPoolMinCount(int[] bufPoolMinCount) {
        this.bufPoolMinCount = bufPoolMinCount;
    }

    public int[] getBufPoolMaxCount() {
        return bufPoolMaxCount;
    }

    public void setBufPoolMaxCount(int[] bufPoolMaxCount) {
        this.bufPoolMaxCount = bufPoolMaxCount;
    }

    public long getBufPoolTimeout() {
        return bufPoolTimeout;
    }

    public void setBufPoolTimeout(long bufPoolTimeout) {
        this.bufPoolTimeout = bufPoolTimeout;
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }
}

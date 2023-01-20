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

import com.github.dtprj.dongting.buf.SimpleByteBufferPool;

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

    private long selectTimeoutMillis = 50;
    private long cleanIntervalMills = 100;
    private long closeTimeoutMillis = 1000;

    private int maxFrameSize = 5 * 1024 * 1024;
    private int maxBodySize = 4 * 1024 * 1024;

    private int[] bufPoolSize = SimpleByteBufferPool.DEFAULT_BUF_SIZE;
    private int[] bufPoolMinCount = SimpleByteBufferPool.DEFAULT_MIN_COUNT;
    private int[] bufPoolMaxCount = SimpleByteBufferPool.DEFAULT_MAX_COUNT;
    private long bufPoolTimeout = SimpleByteBufferPool.DEFAULT_TIME_OUT_MILLIS;

    private int readBufferSize = 128 * 1024;
    private long readBufferTimeoutMillis = 200;

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

    public long getSelectTimeoutMillis() {
        return selectTimeoutMillis;
    }

    public void setSelectTimeoutMillis(long selectTimeoutMillis) {
        this.selectTimeoutMillis = selectTimeoutMillis;
    }

    public long getCleanIntervalMills() {
        return cleanIntervalMills;
    }

    public void setCleanIntervalMills(long cleanIntervalMills) {
        this.cleanIntervalMills = cleanIntervalMills;
    }

    public long getCloseTimeoutMillis() {
        return closeTimeoutMillis;
    }

    public void setCloseTimeoutMillis(long closeTimeoutMillis) {
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

    public int getMaxBodySize() {
        return maxBodySize;
    }

    public void setMaxBodySize(int maxBodySize) {
        this.maxBodySize = maxBodySize;
    }

    public int getReadBufferSize() {
        return readBufferSize;
    }

    public void setReadBufferSize(int readBufferSize) {
        this.readBufferSize = readBufferSize;
    }

    public long getReadBufferTimeoutMillis() {
        return readBufferTimeoutMillis;
    }

    public void setReadBufferTimeoutMillis(long readBufferTimeoutMillis) {
        this.readBufferTimeoutMillis = readBufferTimeoutMillis;
    }
}

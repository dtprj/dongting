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

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.buf.TwoLevelPool;
import com.github.dtprj.dongting.common.Timestamp;

import java.util.function.BiFunction;

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

    private long selectTimeout = 50;
    private long cleanInterval = 100;

    private int maxFrameSize = 5 * 1024 * 1024;
    private int maxBodySize = 4 * 1024 * 1024;

    private BiFunction<Timestamp, Boolean, ByteBufferPool> poolFactory = TwoLevelPool.getDefaultFactory();

    private int readBufferSize = 128 * 1024;
    private long readBufferTimeout = 200;

    private boolean finishPendingImmediatelyWhenChannelClose = false;

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

    public long getSelectTimeout() {
        return selectTimeout;
    }

    public void setSelectTimeout(long selectTimeout) {
        this.selectTimeout = selectTimeout;
    }

    public long getCleanInterval() {
        return cleanInterval;
    }

    public void setCleanInterval(long cleanInterval) {
        this.cleanInterval = cleanInterval;
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

    public BiFunction<Timestamp, Boolean, ByteBufferPool> getPoolFactory() {
        return poolFactory;
    }

    public void setPoolFactory(BiFunction<Timestamp, Boolean, ByteBufferPool> poolFactory) {
        this.poolFactory = poolFactory;
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

    public long getReadBufferTimeout() {
        return readBufferTimeout;
    }

    public void setReadBufferTimeout(long readBufferTimeout) {
        this.readBufferTimeout = readBufferTimeout;
    }

    public boolean isFinishPendingImmediatelyWhenChannelClose() {
        return finishPendingImmediatelyWhenChannelClose;
    }

    public void setFinishPendingImmediatelyWhenChannelClose(boolean finishPendingImmediatelyWhenChannelClose) {
        this.finishPendingImmediatelyWhenChannelClose = finishPendingImmediatelyWhenChannelClose;
    }
}

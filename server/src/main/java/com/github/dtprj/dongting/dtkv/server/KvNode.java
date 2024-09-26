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
package com.github.dtprj.dongting.dtkv.server;

/**
 * @author huangli
 */
public class KvNode {
    protected final byte[] data;
    protected final long createIndex;
    protected final long createTime;

    protected long updateIndex;
    protected long updateTime;

    protected final boolean dir;


    public KvNode(long createIndex, long createTime, boolean dir, byte[] data) {
        this.createIndex = createIndex;
        this.createTime = createTime;
        this.data = data;
        this.dir = dir;
    }

    public byte[] getData() {
        return data;
    }

    public long getCreateIndex() {
        return createIndex;
    }

    public boolean isDir() {
        return dir;
    }

    public long getUpdateIndex() {
        return updateIndex;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }
}

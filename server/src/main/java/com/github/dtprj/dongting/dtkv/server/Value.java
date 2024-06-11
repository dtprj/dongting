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
class Value {
    private final String key;
    private final byte[] data;
    private final long raftIndex;

    private Value previous;
    private boolean evicted;

    public Value(long raftIndex,String key, byte[] data) {
        this.raftIndex = raftIndex;
        this.data = data;
        this.key = key;
    }

    public byte[] getData() {
        return data;
    }

    public long getRaftIndex() {
        return raftIndex;
    }

    public Value getPrevious() {
        return previous;
    }

    public void setPrevious(Value previous) {
        this.previous = previous;
    }

    public boolean isEvicted() {
        return evicted;
    }

    public void setEvicted(boolean evicted) {
        this.evicted = evicted;
    }

    public String getKey() {
        return key;
    }
}

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
package com.github.dtprj.dongting.dtkv;

import com.github.dtprj.dongting.buf.RefBuffer;
import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.raft.client.RaftException;
import com.github.dtprj.dongting.raft.sm.Snapshot;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

/**
 * @author huangli
 */
public class KvSnapshot extends Snapshot {
    private final Runnable statusChecker;
    private final RefBufferFactory heapPool;
    private final Consumer<Snapshot> closeCallback;
    private final Iterator<Map.Entry<String, Value>> iterator;

    public KvSnapshot(long lastIncludedIndex, int lastIncludedTerm, Runnable statusChecker,
                      ConcurrentSkipListMap<String, Value> map, RefBufferFactory heapPool,
                      Consumer<Snapshot> closeCallback) {
        super(lastIncludedIndex, lastIncludedTerm);
        this.statusChecker = statusChecker;
        this.heapPool = heapPool;
        this.closeCallback = closeCallback;
        this.iterator = map.entrySet().iterator();
    }

    @Override
    public CompletableFuture<RefBuffer> readNext() {
        try {
            statusChecker.run();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        RefBuffer refBuffer = null;
        Map.Entry<String, Value> en;
        while ((en = iterator.next()) != null) {
            Value value = en.getValue();
            while (value != null && value.getRaftIndex() > lastIncludedIndex) {
                value = value.getPrevious();
            }
            if (value == null) {
                continue;
            }
            String key = en.getKey();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] data = value.getData();
            int len = 4 + keyBytes.length + 4 + data.length;
            if (len < 0) {
                return CompletableFuture.failedFuture(new RaftException("key + value overflow"));
            }
            if (refBuffer == null) {
                refBuffer = heapPool.create(Math.min(128 * 1024, len));
            }
            ByteBuffer bb = refBuffer.getBuffer();
            if (bb.remaining() < len) {
                return CompletableFuture.completedFuture(refBuffer);
            }
            bb.putInt(keyBytes.length);
            bb.put(keyBytes);
            bb.putInt(data.length);
            bb.put(data);
        }
        return CompletableFuture.completedFuture(refBuffer);
    }

    @Override
    protected void doClose() {
        closeCallback.accept(this);
    }
}

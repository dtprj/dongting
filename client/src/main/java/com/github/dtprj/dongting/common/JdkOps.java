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
package com.github.dtprj.dongting.common;

import com.github.dtprj.dongting.net.NioStatus;
import com.github.dtprj.dongting.queue.MpscLinkedQueue;

import java.nio.ByteBuffer;

/**
 * JDK version dependent operations; impl classes are loaded by {@link DtUtil#JDK_OPS}.
 *
 * @author huangli
 */
public abstract class JdkOps {

    public abstract AbstractRefCountUpdater newRefCountUpdater(boolean plain);

    public abstract <E> MpscLinkedQueue<E> newMpscLinkedQueue();

    public abstract void releaseDirectBuffer(ByteBuffer buffer);

    public abstract void releaseFence();

    public abstract void acquireFence();

    public abstract void fullFence();

    public abstract void onSpinWait();

    public abstract NioStatus newNioStatus();

    /**
     * Reads a big-endian int from the byte array.
     */
    public abstract int getInt(byte[] src, int offset);

    /**
     * Reads a big-endian long from the byte array.
     */
    public abstract long getLong(byte[] src, int offset);
}

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

import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.pb.PbUtil;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;

class ChannelOps {

    private static final int INIT_BUF_SIZE = 1024;
    private static final int MAX_FRAME_SIZE = 8 * 1024 * 1024;
    private final PbCallback callback;

    private WeakReference<ByteBuffer> readBufferCache;

    private int currentReadFrameSize = -1;
    private int readBufferMark = 0;
    private ByteBuffer readBuffer;

    public ChannelOps(PbCallback callback) {
        this.callback = callback;
    }

    public ByteBuffer getOrCreateReadBuffer() {
        if (readBuffer != null) {
            return readBuffer;
        }
        if (readBufferCache == null) {
            readBuffer = ByteBuffer.allocateDirect(INIT_BUF_SIZE);
            readBufferCache = new WeakReference<>(readBuffer);
        } else {
            ByteBuffer cached = readBufferCache.get();
            if (cached != null) {
                readBuffer = cached;
            } else {
                readBuffer = ByteBuffer.allocateDirect(INIT_BUF_SIZE);
                readBufferCache = new WeakReference<>(readBuffer);
            }
        }
        return readBuffer;
    }

    public void afterRead() {
        ByteBuffer buf = this.readBuffer;
        // switch to read mode, but the start position may not 0, so we can't use flip()
        buf.limit(buf.position());
        buf.position(this.readBufferMark);

        for (int rest = buf.remaining(); rest > this.currentReadFrameSize; rest = buf.remaining()) {
            if (currentReadFrameSize == -1) {
                // read frame length
                if (rest >= 4) {
                    currentReadFrameSize = buf.getInt();
                    if (currentReadFrameSize <= 0 || currentReadFrameSize > MAX_FRAME_SIZE) {
                        throw new DtException("frame too large: " + currentReadFrameSize);
                    }
                    readBufferMark = buf.position();
                } else {
                    // wait next read
                }
            }
            if (currentReadFrameSize > 0) {
                readFrame(buf);
            }
        }

        prepareForNextRead();
    }

    private void readFrame(ByteBuffer buf) {
        if (buf.remaining() >= this.currentReadFrameSize) {
            int limit = buf.limit();
            buf.limit(buf.position() + this.currentReadFrameSize);
            PbUtil.parse(buf, callback);
            buf.limit(limit);
            this.readBufferMark = buf.position();
            this.currentReadFrameSize = -1;
        }
    }

    private void prepareForNextRead() {
        ByteBuffer buf = readBuffer;
        int endIndex = buf.limit();
        int capacity = buf.capacity();

        int frameSize = this.currentReadFrameSize;
        int mark = this.readBufferMark;
        if (frameSize == -1) {
            if (mark == capacity) {
                // read complete and buffer exhausted
                buf.clear();
                this.readBufferMark = 0;
            } else {
                int c = endIndex - mark;
                assert c >= 0 && c < 4;
                if (capacity - endIndex < 4 - c) {
                    // move length bytes to start
                    for (int i = 0; i < c; i++) {
                        buf.put(i, buf.get(mark + i));
                    }
                    buf.position(c);
                    buf.limit(capacity);
                    this.readBufferMark = 0;
                } else {
                    // return, continue read
                }
            }
        } else {
            int c = endIndex - mark;
            if (capacity - endIndex >= frameSize - c) {
                // there is enough space for this frame, return, continue read
            } else {
                buf.limit(endIndex);
                buf.position(mark);
                if (frameSize > capacity) {
                    // allocate bigger buffer
                    ByteBuffer newBuffer = ByteBuffer.allocateDirect(frameSize);
                    buf.put(newBuffer);
                    this.readBuffer = newBuffer;
                    this.readBufferCache = new WeakReference<>(newBuffer);
                } else {
                    // copy bytes to start
                    ByteBuffer newBuffer = buf.slice();
                    buf.clear();
                    newBuffer.put(buf);
                }
                this.readBufferMark = 0;
            }
        }
    }
}

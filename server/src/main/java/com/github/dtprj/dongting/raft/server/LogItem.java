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
package com.github.dtprj.dongting.raft.server;

import com.github.dtprj.dongting.buf.ByteBufferPool;
import com.github.dtprj.dongting.common.RefCount;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class LogItem extends RefCount {
    public static final int TYPE_NORMAL = 0;
    public static final int TYPE_HEARTBEAT = 1;
    public static final int TYPE_PREPARE_CONFIG_CHANGE = 2;
    public static final int TYPE_DROP_CONFIG_CHANGE = 3;
    public static final int TYPE_COMMIT_CONFIG_CHANGE = 4;

    private final ByteBufferPool heapPool;

    private int type;
    private int bizType;
    private int term;
    private long index;
    private int prevLogTerm;
    private long timestamp;

    private ByteBuffer bodyBuffer;
    private Object body;
    private int actualBodySize;

    private ByteBuffer headerBuffer;
    private Object header;
    private int actualHeaderSize;

    private int itemHeaderSize;
    private int itemSize;

    public LogItem(ByteBufferPool heapPool) {
        super(true);
        this.heapPool = heapPool;
    }

    @Override
    protected void doClean() {
        if (headerBuffer != null) {
            heapPool.release(headerBuffer);
        }
        if (bodyBuffer != null) {
            heapPool.release(bodyBuffer);
        }
        if (header instanceof RefCount) {
            ((RefCount) header).release();
        }
        if (body instanceof RefCount) {
            ((RefCount) body).release();
        }
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getBizType() {
        return bizType;
    }

    public void setBizType(int bizType) {
        this.bizType = bizType;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public Object getBody() {
        return body;
    }

    public void setBody(Object body) {
        this.body = body;
    }

    public int getActualBodySize() {
        return actualBodySize;
    }

    public void setActualBodySize(int actualBodySize) {
        this.actualBodySize = actualBodySize;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public ByteBuffer getBodyBuffer() {
        return bodyBuffer;
    }

    public void setBodyBuffer(ByteBuffer bodyBuffer) {
        this.bodyBuffer = bodyBuffer;
    }

    public Object getHeader() {
        return header;
    }

    public void setHeader(Object header) {
        this.header = header;
    }

    public int getActualHeaderSize() {
        return actualHeaderSize;
    }

    public void setActualHeaderSize(int actualHeaderSize) {
        this.actualHeaderSize = actualHeaderSize;
    }

    public ByteBuffer getHeaderBuffer() {
        return headerBuffer;
    }

    public void setHeaderBuffer(ByteBuffer headerBuffer) {
        this.headerBuffer = headerBuffer;
    }

    public int getItemHeaderSize() {
        return itemHeaderSize;
    }

    public void setItemHeaderSize(int itemHeaderSize) {
        this.itemHeaderSize = itemHeaderSize;
    }

    public int getItemSize() {
        return itemSize;
    }

    public void setItemSize(int itemSize) {
        this.itemSize = itemSize;
    }
}

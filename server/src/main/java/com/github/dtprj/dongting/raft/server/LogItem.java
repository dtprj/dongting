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

import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.common.RefCount;

/**
 * @author huangli
 */
public class LogItem {
    //message LogItem {
    //    int32 type = 1;
    //    int32 bizType = 2;
    //    int32 term = 3;
    //    fixed64 index = 4;
    //    int32 prev_log_term = 5;
    //    fixed64 timestamp = 6;
    //    bytes header = 7;
    //    bytes body = 8;
    //}
    public static final int IDX_TYPE = 1;
    public static final int IDX_BIZ_TYPE = 2;
    public static final int IDX_TERM = 3;
    public static final int IDX_INDEX = 4;
    public static final int IDX_PREV_LOG_TERM = 5;
    public static final int IDX_TIMESTAMP = 6;
    public static final int IDX_HEADER = 7;
    public static final int IDX_BODY = 8;

    public static final int TYPE_NORMAL = 0;
    public static final int TYPE_HEARTBEAT = 1;
    public static final int TYPE_PREPARE_CONFIG_CHANGE = 2;
    public static final int TYPE_DROP_CONFIG_CHANGE = 3;
    public static final int TYPE_COMMIT_CONFIG_CHANGE = 4;
    public static final int TYPE_LOG_READ = 5;

    private int type;
    private int bizType;
    private int term;
    private long index;
    private int prevLogTerm;
    private long timestamp;

    private Encodable body;
    private boolean bodyIsRefCount;
    private int actualBodySize = -1;

    private Encodable header;
    private boolean headerIsRefCount;
    private int actualHeaderSize = -1;

    private int pbHeaderSize;
    private int pbItemSize;

    public LogItem() {
    }

    public void retain() {
        if (headerIsRefCount) {
            ((RefCount) header).retain();
        }
        if (bodyIsRefCount) {
            ((RefCount) body).retain();
        }
    }

    public void release() {
        if (headerIsRefCount) {
            ((RefCount) header).release();
        }
        if (bodyIsRefCount) {
            ((RefCount) body).release();
        }
    }

    public void setHeader(Encodable header, boolean refCount) {
        this.header = header;
        this.headerIsRefCount = refCount;
    }

    public void setBody(Encodable body, boolean refCount) {
        this.body = body;
        this.bodyIsRefCount = refCount;
    }

    public void setHeader(Encodable header) {
        this.header = header;
        this.headerIsRefCount = header instanceof RefCount;
    }

    public void setBody(Encodable body) {
        this.body = body;
        this.bodyIsRefCount = body instanceof RefCount;
    }

    public int getActualHeaderSize() {
        if (actualHeaderSize == -1) {
            actualHeaderSize = header == null ? 0 : header.actualSize();
        }
        return actualHeaderSize;
    }

    public int getActualBodySize() {
        if (actualBodySize == -1) {
            actualBodySize = body == null ? 0 : body.actualSize();
        }
        return actualBodySize;
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

    public Encodable getBody() {
        return body;
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

    public Encodable getHeader() {
        return header;
    }

    public void setActualHeaderSize(int actualHeaderSize) {
        this.actualHeaderSize = actualHeaderSize;
    }

    public int getPbItemSize() {
        return pbItemSize;
    }

    public void setPbItemSize(int pbItemSize) {
        this.pbItemSize = pbItemSize;
    }

    public int getPbHeaderSize() {
        return pbHeaderSize;
    }

    public void setPbHeaderSize(int pbHeaderSize) {
        this.pbHeaderSize = pbHeaderSize;
    }

}

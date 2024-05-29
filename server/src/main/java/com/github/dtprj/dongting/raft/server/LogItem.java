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
public class LogItem extends RefCount {
    public static final int TYPE_NORMAL = 0;
    public static final int TYPE_HEARTBEAT = 1;
    public static final int TYPE_PREPARE_CONFIG_CHANGE = 2;
    public static final int TYPE_DROP_CONFIG_CHANGE = 3;
    public static final int TYPE_COMMIT_CONFIG_CHANGE = 4;

    private int type;
    private int bizType;
    private int term;
    private long index;
    private int prevLogTerm;
    private long timestamp;

    private Encodable body;
    private int actualBodySize = -1;

    private Encodable header;
    private int actualHeaderSize = -1;

    private int pbHeaderSize;
    private int pbItemSize;

    public LogItem() {
    }

    @Override
    protected void doClean() {
        if (header instanceof RefCount) {
            ((RefCount) header).release();
        }
        header = null;
        if (body instanceof RefCount) {
            ((RefCount) body).release();
        }
        body = null;
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

    public void setBody(Encodable body) {
        this.body = body;
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

    public void setHeader(Encodable header) {
        this.header = header;
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

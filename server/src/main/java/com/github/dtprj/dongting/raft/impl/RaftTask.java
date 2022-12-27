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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.net.RespWriter;

/**
 * @author huangli
 */
public class RaftTask {
    public static final int TYPE_SHUTDOWN = 1;
    public static final int TYPE_APPEND_ENTRIES_REQ = 2;
    public static final int TYPE_APPEND_ENTRIES_RESP = 3;
    public static final int TYPE_REQUEST_VOTE_REQ = 4;
    public static final int TYPE_REQUEST_VOTE_RESP = 5;

    private int type;
    private Object data;
    private RespWriter respWriter;
    private RaftNode remoteNode;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public RespWriter getRespWriter() {
        return respWriter;
    }

    public void setRespWriter(RespWriter respWriter) {
        this.respWriter = respWriter;
    }

    public RaftNode getRemoteNode() {
        return remoteNode;
    }

    public void setRemoteNode(RaftNode remoteNode) {
        this.remoteNode = remoteNode;
    }
}

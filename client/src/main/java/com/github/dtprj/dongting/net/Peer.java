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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author huangli
 */
public class Peer {
    private volatile PeerStatus status;
    private final HostPort endPoint;
    private final NioNet owner;
    private DtChannel dtChannel;
    private int connectionId;

    private LinkedList<WriteData> waitConnectList;
    private NioWorker.ConnectInfo connectInfo;

    Peer(HostPort endPoint, NioNet owner) {
        Objects.requireNonNull(endPoint);
        Objects.requireNonNull(owner);
        this.endPoint = endPoint;
        this.owner = owner;
        this.status = PeerStatus.not_connect;
    }

    public HostPort getEndPoint() {
        return endPoint;
    }

    public PeerStatus getStatus() {
        return status;
    }

    public int getConnectionId() {
        return connectionId;
    }

    void addToWaitConnectList(WriteData data) {
        if (waitConnectList == null) {
            waitConnectList = new LinkedList<>();
        }
        waitConnectList.add(data);
    }

    void cleanWaitingConnectList(Function<WriteData, NetException> exceptionSupplier) {
        if (waitConnectList == null) {
            return;
        }
        for (Iterator<WriteData> it = waitConnectList.iterator(); it.hasNext(); ) {
            WriteData wd = it.next();
            NetException ex = exceptionSupplier.apply(wd);
            if (ex != null) {
                it.remove();
                wd.callFail(true, ex);
            } else {
                break;
            }
        }
        if (waitConnectList.isEmpty()) {
            waitConnectList = null;
        }
    }

    void enqueueAfterConnect() {
        if (waitConnectList == null) {
            return;
        }
        for(Iterator<WriteData> it = waitConnectList.iterator();it.hasNext();){
            WriteData wd = it.next();
            it.remove();
            wd.setDtc(dtChannel);
            dtChannel.getSubQueue().enqueue(wd);
        }
        waitConnectList = null;
    }

    NioNet getOwner() {
        return owner;
    }

    DtChannel getDtChannel() {
        return dtChannel;
    }

    void setDtChannel(DtChannel dtChannel) {
        this.dtChannel = dtChannel;
    }

    void setStatus(PeerStatus status) {
        this.status = status;
    }

    void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    NioWorker.ConnectInfo getConnectInfo() {
        return connectInfo;
    }

    void setConnectInfo(NioWorker.ConnectInfo connectInfo) {
        this.connectInfo = connectInfo;
    }
}

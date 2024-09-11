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
    volatile PeerStatus status;
    private final HostPort endPoint;
    final NioNet owner;
    DtChannelImpl dtChannel;
    int connectionId;
    NioWorker.ConnectInfo connectInfo;

    private LinkedList<WriteData> waitConnectList;

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

}

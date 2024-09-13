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
    private final HostPort endPoint;
    final NioClient owner;

    volatile PeerStatus status;
    DtChannelImpl dtChannel;

    int connectionId;
    NioWorker.ConnectInfo connectInfo;

    boolean needConnect;
    int retry;
    long lastConnectFailNanos;

    private LinkedList<WriteData> waitConnectList;

    Peer(HostPort endPoint, NioClient owner) {
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
        for (Iterator<WriteData> it = waitConnectList.iterator(); it.hasNext(); ) {
            WriteData wd = it.next();
            it.remove();
            wd.setDtc(dtChannel);
            dtChannel.getSubQueue().enqueue(wd);
        }
        waitConnectList = null;
    }

    void markNotConnect(NioConfig config, WorkerStatus workerStatus) {
        NioClientConfig c = (NioClientConfig) config;
        if (needConnect) {
            long base;
            int index;
            if (retry == 0) {
                base = workerStatus.ts.getNanoTime();
                index = 0;
                workerStatus.retryConnect++;
            } else {
                base = lastConnectFailNanos;
                index = retry > c.getConnectRetryIntervals().length - 1 ?
                        c.getConnectRetryIntervals().length - 1 : retry;
            }
            retry = retry + 1 > 0 ? retry + 1 : Integer.MAX_VALUE;
            lastConnectFailNanos = base + c.getConnectRetryIntervals()[index] * 1_000_000;
        } else {
            resetConnectRetry(workerStatus);
        }
        status = PeerStatus.not_connect;
    }

    void resetConnectRetry(WorkerStatus workerStatus) {
        if (retry > 0) {
            workerStatus.retryConnect--;
        }
        retry = 0;
        lastConnectFailNanos = 0;
    }
}

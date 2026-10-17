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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.util.LinkedList;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author huangli
 */
public class Peer {
    private static final DtLog log = DtLogs.getLogger(Peer.class);
    public volatile PeerStatus status;
    public final HostPort endPoint;
    public int connectRetryCount; // reset to 0 when connect success

    final NioClient owner;

    DtChannelImpl dtChannel;

    NioWorker.ConnectInfo connectInfo;

    boolean shouldAutoReconnect;
    long lastRetryNanos;

    private LinkedList<PacketInfoReq> waitConnectList;

    Peer(HostPort endPoint, NioClient owner) {
        Objects.requireNonNull(endPoint);
        Objects.requireNonNull(owner);
        this.endPoint = endPoint;
        this.owner = owner;
        this.status = PeerStatus.not_connect;
    }

    void addToWaitConnectList(PacketInfoReq data) {
        if (waitConnectList == null) {
            waitConnectList = new LinkedList<>();
        }
        waitConnectList.add(data);
    }

    void cleanWaitingConnectList(Function<PacketInfoReq, NetException> exceptionSupplier) {
        LinkedList<PacketInfoReq> list = waitConnectList;
        if (list == null) {
            return;
        }
        waitConnectList = null;
        try {
            while (!list.isEmpty()) {
                PacketInfoReq wd = list.peekFirst();
                NetException ex = exceptionSupplier.apply(wd);
                if (ex == null) {
                    break;
                }
                list.pollFirst();
                wd.packet.clean();
                wd.callFail(ex);
            }
        } finally {
            if (!list.isEmpty()) {
                if (waitConnectList == null) {
                    waitConnectList = list;
                } else {
                    list.addAll(waitConnectList);
                    waitConnectList = list;
                }
            }
        }
    }

    void enqueueAfterConnect() {
        LinkedList<PacketInfoReq> list = waitConnectList;
        if (list == null) {
            return;
        }
        waitConnectList = null;
        try {
            PacketInfo wd;
            while ((wd = list.pollFirst()) != null) {
                wd.dtc = dtChannel;
                dtChannel.subQueue.enqueue(wd);
            }
        } finally {
            if (!list.isEmpty()) {
                if (waitConnectList == null) {
                    waitConnectList = list;
                } else {
                    list.addAll(waitConnectList);
                    waitConnectList = list;
                }
            }
        }
    }

    void markNotConnect(NioConfig config, WorkerStatus workerStatus, boolean byAutoRetry) {
        NioClientConfig c = (NioClientConfig) config;
        if (shouldAutoReconnect && c.connectRetryIntervals != null) {
            long base;
            int index;
            if (connectRetryCount == 0) {
                base = workerStatus.ts.nanoTime;
                index = 0;
                workerStatus.retryConnect++;
            } else {
                if (byAutoRetry) {
                    base = lastRetryNanos;
                    index = Math.min(connectRetryCount, c.connectRetryIntervals.length - 1);
                } else {
                    base = 0;
                    index = -1;
                }
            }
            if (index != -1) {
                long millis = c.connectRetryIntervals[index];
                lastRetryNanos = base + millis * 1_000_000;
                connectRetryCount = connectRetryCount + 1 > 0 ? connectRetryCount + 1 : Integer.MAX_VALUE;
                log.info("peer {} connect fail, {}th retry after {}ms", endPoint, connectRetryCount, millis);
            }
        } else {
            resetConnectRetry(workerStatus);
        }
        status = PeerStatus.not_connect;
    }

    void resetConnectRetry(WorkerStatus workerStatus) {
        if (connectRetryCount > 0) {
            workerStatus.retryConnect--;
        }
        connectRetryCount = 0;
        lastRetryNanos = 0;
    }
}

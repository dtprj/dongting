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

import java.util.Objects;

/**
 * @author huangli
 */
public class Peer {
    private final HostPort endPoint;
    private final NioNet owner;
    private DtChannel dtChannel;
    private volatile PeerStatus status;

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

    NioNet getOwner() {
        return owner;
    }

    DtChannel getDtChannel() {
        return dtChannel;
    }

    void setDtChannel(DtChannel dtChannel) {
        this.dtChannel = dtChannel;
    }

    public PeerStatus getStatus() {
        return status;
    }

    void setStatus(PeerStatus status) {
        this.status = status;
    }
}

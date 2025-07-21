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

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

/**
 * @author huangli
 */
public interface DtChannel {
    SocketChannel getChannel();

    SocketAddress getRemoteAddr();

    SocketAddress getLocalAddr();

    /**
     * server side will return null.
     */
    Peer getPeer();

    long getLastActiveTimeNanos();

    NioNet getOwner();

    /**
     * client side will return 0.
     */
    long getRemoteUuid1();

    /**
     * client side will return 0.
     */
    long getRemoteUuid2();
}

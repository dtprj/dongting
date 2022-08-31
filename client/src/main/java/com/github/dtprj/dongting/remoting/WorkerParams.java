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
package com.github.dtprj.dongting.remoting;

import java.nio.channels.SocketChannel;
import java.util.HashMap;

public class WorkerParams {
    private RpcPbCallback callback;
    private SocketChannel channel;
    private IoQueue ioQueue;
    private Runnable wakeupRunnable;
    private HashMap<Integer, WriteObj> pendingRequests;

    public RpcPbCallback getCallback() {
        return callback;
    }

    public void setCallback(RpcPbCallback callback) {
        this.callback = callback;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public void setChannel(SocketChannel channel) {
        this.channel = channel;
    }

    public IoQueue getIoQueue() {
        return ioQueue;
    }

    public void setIoQueue(IoQueue ioQueue) {
        this.ioQueue = ioQueue;
    }

    public Runnable getWakeupRunnable() {
        return wakeupRunnable;
    }

    public void setWakeupRunnable(Runnable wakeupRunnable) {
        this.wakeupRunnable = wakeupRunnable;
    }

    public HashMap<Integer, WriteObj> getPendingRequests() {
        return pendingRequests;
    }

    public void setPendingRequests(HashMap<Integer, WriteObj> pendingRequests) {
        this.pendingRequests = pendingRequests;
    }
}

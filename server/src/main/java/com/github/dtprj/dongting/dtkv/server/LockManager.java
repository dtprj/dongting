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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.common.ByteArray;
import com.github.dtprj.dongting.common.DtTime;
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.DtChannel;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.NioServer;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Lock notification manager
 * @author huangli
 */
class LockManager {

    private static final DtLog log = DtLogs.getLogger(LockManager.class);
    
    /**
     * Send lock notification to the new lock owner
     * @param nioServer the nio server instance
     * @param lockKey the key of the new lock owner
     * @param groupId the raft group id
     */
    public static void notifyNewLockOwner(NioServer nioServer, ByteArray lockKey, int groupId) {
        
        UUID ownerUuid = KvServerUtil.parseLockKeyUuid(lockKey);
        if (ownerUuid == null) {
            return;
        }
        
        DtChannel channel = nioServer.getClients().get(ownerUuid);
        if (channel == null) {
            // Client is not connected, skip notification
            return;
        }
        
        KvReq req = new KvReq();
        req.groupId = groupId;
        req.key = lockKey.getData();
        EncodableBodyWritePacket packet = new EncodableBodyWritePacket(Commands.DTKV_LOCK_PUSH, req);
        DtTime timeout = new DtTime(5, TimeUnit.SECONDS);
        
        // Send one-way message, don't wait for response
        nioServer.sendOneWay(channel, packet, timeout, (result, ex) -> {
            if (ex != null) {
                log.warn("Failed to notify new lock owner: channel={}, error={}", channel.getChannel(),
                        ex.getMessage());
            }
        });
    }
}
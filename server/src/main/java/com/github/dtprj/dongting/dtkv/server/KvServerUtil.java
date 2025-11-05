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
import com.github.dtprj.dongting.dtkv.KvClientConfig;
import com.github.dtprj.dongting.dtkv.KvCodes;
import com.github.dtprj.dongting.dtkv.KvReq;
import com.github.dtprj.dongting.dtkv.KvResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.DtChannel;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.EncodableBodyWritePacket;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.raft.impl.RaftGroupImpl;
import com.github.dtprj.dongting.raft.impl.RaftStatusImpl;
import com.github.dtprj.dongting.raft.server.RaftGroup;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author huangli
 */
public class KvServerUtil {

    private static final DtLog log = DtLogs.getLogger(KvServerUtil.class);

    /**
     * call after RaftServer init, before RaftServer start
     */
    public static void initKvServer(RaftServer server) {
        NioServer nioServer = server.getServiceNioServer();

        KvProcessor p = new KvProcessor(server);
        nioServer.register(Commands.DTKV_GET, p);
        nioServer.register(Commands.DTKV_PUT, p);
        nioServer.register(Commands.DTKV_REMOVE, p);
        nioServer.register(Commands.DTKV_MKDIR, p);
        nioServer.register(Commands.DTKV_LIST, p);
        nioServer.register(Commands.DTKV_BATCH_GET, p);
        nioServer.register(Commands.DTKV_BATCH_PUT, p);
        nioServer.register(Commands.DTKV_BATCH_REMOVE, p);
        nioServer.register(Commands.DTKV_CAS, p);

        nioServer.register(Commands.DTKV_SYNC_WATCH, new WatchProcessor(server));
        nioServer.register(Commands.DTKV_QUERY_STATUS, new KvStatusProcessor(server));

        nioServer.register(Commands.DTKV_PUT_TEMP_NODE, p);
        nioServer.register(Commands.DTKV_MAKE_TEMP_DIR, p);
        nioServer.register(Commands.DTKV_UPDATE_TTL, p);

        nioServer.register(Commands.DTKV_TRY_LOCK, p);
        nioServer.register(Commands.DTKV_UNLOCK, p);
    }

    static DtKV getStateMachine(ReqInfo<?> reqInfo) {
        StateMachine sm = reqInfo.raftGroup.getStateMachine();
        try {
            return (DtKV) sm;
        } catch (ClassCastException e) {
            EmptyBodyRespPacket errorResp = new EmptyBodyRespPacket(CmdCodes.CLIENT_ERROR);
            errorResp.msg = "type error: " + sm;
            reqInfo.reqContext.writeRespInBizThreads(errorResp);
            return null;
        }
    }

    private static final byte[] HEX = new byte[]{
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    static ByteArray buildLockKey(ByteArray parent, long uuid1, long uuid2) {
        byte[] pb = parent.getData();
        byte[] lockKey = new byte[pb.length + 33];
        System.arraycopy(pb, 0, lockKey, 0, pb.length);
        int pos = pb.length;
        lockKey[pos++] = KvClientConfig.SEPARATOR;
        for (int shiftBits = 60; shiftBits >= 0; shiftBits -= 4) {
            lockKey[pos++] = HEX[(int) ((uuid1 >>> shiftBits) & 0xF)];
        }
        for (int shiftBits = 60; shiftBits >= 0; shiftBits -= 4) {
            lockKey[pos++] = HEX[(int) ((uuid2 >>> shiftBits) & 0xF)];
        }
        return new ByteArray(lockKey);
    }

    /**
     * Parse UUID from lock key
     *
     * @param lockKey the lock key created by buildLockKey
     * @return UUID or null if parsing failed
     */
    static UUID parseLockKeyUuid(ByteArray lockKey) {
        byte[] keyBytes = lockKey.getData();
        if (keyBytes.length < 33) {
            return null;
        }

        // Check if the separator is at the expected position (倒数33位)
        int separatorPos = keyBytes.length - 33;
        if (keyBytes[separatorPos] != KvClientConfig.SEPARATOR) {
            return null;
        }

        long uuid1 = 0;
        long uuid2 = 0;
        int pos = separatorPos + 1;

        // Parse first part of UUID (16 hex chars)
        for (int i = 0; i < 16; i++) {
            int hexValue = hexCharToValue(keyBytes[pos++]);
            if (hexValue == -1) {
                return null; // Invalid hex character
            }
            uuid1 = (uuid1 << 4) | hexValue;
        }

        // Parse second part of UUID (16 hex chars)
        for (int i = 0; i < 16; i++) {
            int hexValue = hexCharToValue(keyBytes[pos++]);
            if (hexValue == -1) {
                return null; // Invalid hex character
            }
            uuid2 = (uuid2 << 4) | hexValue;
        }

        return new UUID(uuid1, uuid2);
    }

    private static int hexCharToValue(byte hexChar) {
        if (hexChar >= '0' && hexChar <= '9') {
            return hexChar - '0';
        }
        if (hexChar >= 'a' && hexChar <= 'f') {
            return hexChar - 'a' + 10;
        }
        return -1; // Invalid hex character
    }

    static void notifyNewLockOwner(RaftGroup g, KvImpl.KvResultWithNewOwnerInfo ri) {
        KvResult r = ri.result;

        if (r.getBizCode() != KvCodes.SUCCESS || ri.newOwner == null || ri.newOwnerData == null) {
            return;
        }

        // Get NioServer from RaftStatusImpl
        RaftStatusImpl raftStatus = ((RaftGroupImpl) g).groupComponents.raftStatus;

        ByteArray lockKey = ri.newOwner.ttlInfo.key;
        UUID ownerUuid = parseLockKeyUuid(lockKey);
        if (ownerUuid == null) {
            return;
        }

        DtChannel channel = raftStatus.serviceNioServer.getClients().get(ownerUuid);
        if (channel == null) {
            log.warn("the owner for key {} is not online, skip notification", lockKey);
            // Client is not connected, skip notification
            return;
        }

        KvReq req = new KvReq();
        req.groupId = g.getGroupId();
        req.key = lockKey.getData();
        req.value = ri.newOwnerData;
        req.ttlMillis = ri.newOwnerServerSideWaitNanos;
        EncodableBodyWritePacket packet = new EncodableBodyWritePacket(Commands.DTKV_LOCK_PUSH, req);
        DtTime timeout = new DtTime(5, TimeUnit.SECONDS);

        // Send one-way message, don't wait for response
        raftStatus.serviceNioServer.sendOneWay(channel, packet, timeout, (result, ex) -> {
            if (ex != null) {
                log.warn("Failed to notify new lock owner: channel={}, error={}", channel.getChannel(),
                        ex.getMessage());
            }
        });
    }
}

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
import com.github.dtprj.dongting.dtkv.KvClient;
import com.github.dtprj.dongting.net.CmdCodes;
import com.github.dtprj.dongting.net.Commands;
import com.github.dtprj.dongting.net.EmptyBodyRespPacket;
import com.github.dtprj.dongting.net.NioServer;
import com.github.dtprj.dongting.raft.server.RaftServer;
import com.github.dtprj.dongting.raft.server.ReqInfo;
import com.github.dtprj.dongting.raft.sm.StateMachine;

import java.util.UUID;

/**
 * @author huangli
 */
public class KvServerUtil {

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

        nioServer.register(Commands.DTKV_LOCK, p);
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
        lockKey[pos++] = KvClient.SEPARATOR;
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
        if (keyBytes[separatorPos] != KvClient.SEPARATOR) {
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
}

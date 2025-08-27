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
package com.github.dtprj.dongting.dtkv;

import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.common.ByteArray;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author huangli
 */
public class WatchReq implements Encodable {

    public static final int IDX_GROUP_ID = 1;
    public static final int IDX_SYNC_ALL = 2;
    public static final int IDX_KEYS_SIZE = 3;
    public static final int IDX_KNOWN_RAFT_INDEXES = 4;
    public static final int IDX_KEYS = 5;

    public final int groupId;
    public final boolean syncAll;
    public final long[] knownRaftIndexes;
    public final List<ByteArray> keys;

    public WatchReq(int groupId, boolean syncAll, List<ByteArray> keys, long[] knownRaftIndexes) {
        if (!syncAll && (keys == null || keys.isEmpty())) {
            throw new IllegalArgumentException("keys size must > 0");
        }
        if (keys != null && (knownRaftIndexes == null || knownRaftIndexes.length != keys.size())) {
            throw new IllegalArgumentException("array length not match");
        }
        this.groupId = groupId;
        this.syncAll = syncAll;
        this.knownRaftIndexes = knownRaftIndexes;
        this.keys = keys;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_GROUP_ID, groupId)) {
                    return false;
                }
                // fall through
            case IDX_GROUP_ID:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_SYNC_ALL, syncAll ? 1 : 0)) {
                    return false;
                }
                // fall through
            case IDX_SYNC_ALL:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_KEYS_SIZE, keys.size())) {
                    return false;
                }
                // fall through
            case IDX_KEYS_SIZE:
                if (!EncodeUtil.encodeFix64s(context, destBuffer, IDX_KNOWN_RAFT_INDEXES, knownRaftIndexes)) {
                    return false;
                }
                // fall through
            case IDX_KNOWN_RAFT_INDEXES:
                return EncodeUtil.encodeList(context, destBuffer, IDX_KEYS, keys);
            default:
                throw new IllegalStateException("stage=" + context.stage);
        }
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupId)
                + PbUtil.sizeOfInt32Field(IDX_SYNC_ALL, syncAll ? 1 : 0)
                + PbUtil.sizeOfInt32Field(IDX_KEYS_SIZE, keys.size())
                + PbUtil.sizeOfFix64Field(IDX_KNOWN_RAFT_INDEXES, knownRaftIndexes)
                + EncodeUtil.sizeOfList(IDX_KEYS, keys);
    }
}

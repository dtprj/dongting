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
package com.github.dtprj.dongting.raft.rpc;

import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.ParseUtil;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class AdminListGroupsResp implements PbCallback<AdminListGroupsResp>, Encodable {

    private static final int IDX_SIZE = 1;
    private static final int IDX_GROUP_ID = 2;

    public int[] groupIds;
    private int writePos = 0;

    @Override
    public AdminListGroupsResp getResult(DecodeContext context) {
        if (groupIds == null) {
            groupIds = new int[0];
        } else {
            ParseUtil.checkCountMatch(groupIds.length, writePos, "group_ids");
        }
        return this;
    }

    @Override
    public void readVarNumber(DecodeContext context, int index, long value) {
        if (index == IDX_SIZE && groupIds == null) {
            int len = (int) value;
            if (len < 0 || len > ParseUtil.MAX_COLLECTION_SIZE) {
                throw new CodecException("invalid group size: " + len);
            }
            groupIds = new int[len];
        } else if (index == IDX_GROUP_ID) {
            ParseUtil.checkSizeBefore(groupIds, "group_id");
            ParseUtil.checkIndexInArray(writePos, groupIds.length, "group_id");
            groupIds[writePos++] = (int) value;
        }
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (groupIds != null && !EncodeUtil.encodeInt32(context, destBuffer, IDX_SIZE, groupIds.length)) {
                    return false;
                }
                // fall through
            case IDX_SIZE:
                return groupIds == null || EncodeUtil.encodeInt32s(context, destBuffer, IDX_GROUP_ID, groupIds);
            default:
                throw new CodecException(context);
        }
    }

    @Override
    public int actualSize() {
        if (groupIds == null) {
            return 0;
        }
        return PbUtil.sizeOfInt32Field(IDX_SIZE, groupIds.length)
                + PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupIds);
    }
}

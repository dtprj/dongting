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
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public class AdminListGroupsResp extends PbCallback<AdminListGroupsResp> implements Encodable {
    private static final DtLog log = DtLogs.getLogger(AdminListGroupsResp.class);

    private static final int IDX_SIZE = 1;
    private static final int IDX_GROUP_ID = 2;

    public int[] groupIds;
    private int writePos = 0;

    @Override
    protected AdminListGroupsResp getResult() {
        return this;
    }

    @Override
    public boolean readVarNumber(int index, long value) {
        if (index == IDX_SIZE) {
            groupIds = new int[(int) value];
        } else if (index == IDX_GROUP_ID) {
            if (writePos < groupIds.length) {
                groupIds[writePos++] = (int) value;
            } else {
                log.error("group_id exceed size {}", groupIds.length);
                return false;
            }
        }
        return true;
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

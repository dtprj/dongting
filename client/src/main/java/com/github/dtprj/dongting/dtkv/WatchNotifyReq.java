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

import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author huangli
 */
public class WatchNotifyReq implements Encodable {
    private static final int IDX_GROUP_ID = 1;
    private static final int IDX_LIST_SIZE = 2;
    private static final int IDX_NOTIFY_LIST = 3;

    public final int groupId;
    public final List<WatchNotify> notifyList;

    private int encodeSize;

    public WatchNotifyReq(int groupId, List<WatchNotify> notifyList) {
        this.groupId = groupId;
        this.notifyList = notifyList;
    }

    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_GROUP_ID, groupId)) {
                    return false;
                }
                // fall through
            case IDX_LIST_SIZE:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_LIST_SIZE, notifyList == null ? 0 : notifyList.size())) {
                    return false;
                }
                // fall through
            case IDX_GROUP_ID:
                return EncodeUtil.encodeList(context, destBuffer, IDX_NOTIFY_LIST, notifyList);
            default:
                throw new CodecException(context);
        }
    }

    @Override
    public int actualSize() {
        if (encodeSize == 0) {
            encodeSize = PbUtil.sizeOfInt32Field(IDX_GROUP_ID, groupId)
                    + PbUtil.sizeOfInt32Field(IDX_LIST_SIZE, notifyList == null ? 0 : notifyList.size())
                    + EncodeUtil.sizeOfList(IDX_NOTIFY_LIST, notifyList);
        }
        return encodeSize;
    }

    public static class Callback extends PbCallback<WatchNotifyReq> {
        private int groupId;
        private ArrayList<WatchNotify> notifyList;
        private final WatchNotify.Callback notifyCallback = new WatchNotify.Callback();

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == IDX_GROUP_ID) {
                groupId = (int) value;
            } else if (index == IDX_LIST_SIZE) {
                notifyList = new ArrayList<>(notifyList);
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == IDX_NOTIFY_LIST) {
                WatchNotify wn = parseNested(buf, fieldLen, currentPos, notifyCallback);
                if (wn != null) {
                    if (notifyList == null) {
                        notifyList = new ArrayList<>();
                    }
                    notifyList.add(wn);
                }
            }
            return true;
        }

        @Override
        protected WatchNotifyReq getResult() {
            return new WatchNotifyReq(groupId, notifyList == null ? Collections.emptyList() : notifyList);
        }
    }
}

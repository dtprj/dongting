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

import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public abstract class WriteFrame extends Frame {
    private static final DtLog log = DtLogs.getLogger(WriteFrame.class);

    private static final int STATUS_INIT = 0;
    private static final int STATUS_HEADER_ENCODE_FINISHED = 1;
    private static final int STATUS_ENCODE_FINISHED = 2;

    private int dumpSize;
    private int bodySize;
    private int status;

    private byte[] msgBytes;

    private static final int MAX_HEADER_SIZE = 4 // length
            + 1 + 1 // uint32 frame_type = 1;
            + 1 + 5 // uint32 command = 2;
            + 1 + 4 // fixed32 seq = 3;
            + 1 + 5 // uint32 resp_code = 4;
            // string resp_msg = 5;
            + 1 + 8; // fixed32 timeout_millis = 6;
    // string extra = 7;


    protected abstract int calcActualBodySize();

    protected abstract boolean encodeBody(EncodeContext context, ByteBuffer buf);

    public final int calcMaxFrameSize() {
        return MAX_HEADER_SIZE
                + (msgBytes == null ? 0 : msgBytes.length)
                + (extra == null ? 0 : extra.length)
                + actualBodySize();
    }

    public final int actualBodySize() {
        int bodySize = this.bodySize;
        if (bodySize == 0) {
            bodySize = calcActualBodySize();
            this.bodySize = bodySize;
        }
        return bodySize;
    }

    @Override
    public void setMsg(String msg) {
        super.setMsg(msg);
        if (msg != null && !msg.isEmpty()) {
            msgBytes = msg.getBytes(StandardCharsets.UTF_8);
        }
    }

    public final int actualSize() {
        int dumpSize = this.dumpSize;
        if (dumpSize == 0) {
            dumpSize = 4 // length
                    + PbUtil.accurateUnsignedIntSize(IDX_TYPE, frameType) // uint32 frame_type = 1;
                    + PbUtil.accurateUnsignedIntSize(IDX_COMMAND, command) // uint32 command = 2;
                    + PbUtil.accurateFix32Size(IDX_SEQ, seq) // fixed32 seq = 3;
                    + PbUtil.accurateUnsignedIntSize(IDX_RESP_CODE, respCode) // uint32 resp_code = 4;
                    + PbUtil.accurateLengthDelimitedSize(IDX_MSG, msgBytes == null ? 0 : msgBytes.length) // string resp_msg = 5;
                    + PbUtil.accurateFix64Size(IDX_TIMOUT, timeout) // fixed64 timeout = 6;
                    + PbUtil.accurateLengthDelimitedSize(IDX_EXTRA, extra == null ? 0 : extra.length) // bytes extra = 7;
                    + PbUtil.accurateLengthDelimitedSize(IDX_BODY, actualBodySize()); // bytes body = 15;
            this.dumpSize = dumpSize;
        }
        return dumpSize;
    }

    public final boolean encode(EncodeContext context, ByteBuffer buf) {
        if (status == STATUS_INIT) {
            int totalSize = actualSize();
            int headerSize = totalSize - actualBodySize();
            if (buf.remaining() < headerSize) {
                return false;
            } else {
                buf.putInt(totalSize - 4); //not include total length
                PbUtil.writeUnsignedInt32(buf, IDX_TYPE, frameType);
                PbUtil.writeUnsignedInt32(buf, IDX_COMMAND, command);
                PbUtil.writeFix32(buf, IDX_SEQ, seq);
                PbUtil.writeUnsignedInt32(buf, IDX_RESP_CODE, respCode);
                PbUtil.writeUTF8(buf, IDX_MSG, msg);
                PbUtil.writeFix64(buf, IDX_TIMOUT, timeout);
                PbUtil.writeBytes(buf, IDX_EXTRA, extra);
                if (bodySize > 0) {
                    PbUtil.writeLengthDelimitedPrefix(buf, Frame.IDX_BODY, bodySize);
                }
                status = STATUS_HEADER_ENCODE_FINISHED;
            }
        }
        if (status == STATUS_HEADER_ENCODE_FINISHED) {
            boolean finish = false;
            try {
                if (bodySize > 0) {
                    finish = encodeBody(context, buf);
                } else {
                    finish = true;
                }
            } finally {
                if (finish) {
                    status = STATUS_ENCODE_FINISHED;
                }
            }
            return finish;
        } else {
            throw new NetException("invalid status: " + status);
        }
    }

    public final void clean() {
        if (status == STATUS_INIT) {
            return;
        }
        try {
            doClean();
        } catch (Throwable e) {
            log.error("clean error", e);
        } finally {
            status = STATUS_INIT;
        }
    }

    /**
     * may be called in io thread (or other thread).
     */
    protected void doClean() {
    }

}

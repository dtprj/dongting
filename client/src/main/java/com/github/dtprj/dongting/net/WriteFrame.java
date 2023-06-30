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
import com.github.dtprj.dongting.codec.Encoder;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public abstract class WriteFrame extends Frame implements Encoder<WriteFrame> {
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

    protected abstract int calcActualBodySize();

    protected abstract boolean encodeBody(EncodeContext context, ByteBuffer buf);

    public final int calcMaxFrameSize() {
        return MAX_HEADER_SIZE + (msgBytes == null ? 0 : msgBytes.length) + actualBodySize();
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

    @Override
    public final int actualSize(WriteFrame data) {
        int dumpSize = this.dumpSize;
        if (dumpSize == 0) {
            dumpSize = 4 // length
                    + PbUtil.accurateUnsignedIntSize(1, frameType) // uint32 frame_type = 1;
                    + PbUtil.accurateUnsignedIntSize(2, command) // uint32 command = 2;
                    + PbUtil.accurateFix32Size(3, seq) // fixed32 seq = 3;
                    + PbUtil.accurateUnsignedIntSize(4, respCode) // uint32 resp_code = 4;
                    + PbUtil.accurateLengthDelimitedSize(5, msgBytes == null ? 0 : msgBytes.length) // string resp_msg = 5;
                    + PbUtil.accurateFix64Size(6, timeout) // fixed64 timeout = 6;
                    + PbUtil.accurateLengthDelimitedSize(15, actualBodySize()); // bytes body = 15;
            this.dumpSize = dumpSize;
        }
        return dumpSize;
    }

    @Override
    public final boolean encode(EncodeContext context, ByteBuffer buf, WriteFrame data) {
        context.setStatus(null);
        if (status == STATUS_INIT) {
            int totalSize = actualSize(data);
            int headerSize = totalSize - actualBodySize();
            if (buf.remaining() < headerSize) {
                return false;
            } else {
                buf.putInt(totalSize - 4); //not include total length
                PbUtil.writeUnsignedInt32(buf, Frame.IDX_TYPE, frameType);
                PbUtil.writeUnsignedInt32(buf, Frame.IDX_COMMAND, command);
                PbUtil.writeFix32(buf, Frame.IDX_SEQ, seq);
                PbUtil.writeUnsignedInt32(buf, Frame.IDX_RESP_CODE, respCode);
                PbUtil.writeUTF8(buf, Frame.IDX_MSG, msg);
                PbUtil.writeFix64(buf, Frame.IDX_TIMOUT, timeout);
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
            } catch (RuntimeException | Error e) {
                context.setStatus(null);
                throw e;
            } finally {
                if (finish) {
                    context.setStatus(null);
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

    protected void doClean() {
    }

}

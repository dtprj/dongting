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

import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.log.BugLog;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author huangli
 */
public abstract class WritePacket extends Packet implements Encodable {
    private static final DtLog log = DtLogs.getLogger(WritePacket.class);

    static final int STATUS_INIT = 0;
    private static final int STATUS_HEADER_ENCODE_FINISHED = 1;
    private static final int STATUS_ENCODE_FINISHED = 2;

    private int dumpSize;
    private int bodySize;

    private byte[] msgBytes;

    volatile boolean acquirePermit;
    int maxPacketSize;

    boolean use;
    private boolean cleaned;

    private static final int MAX_HEADER_SIZE = 4 // length
            + 1 + 1 // uint32 packet_type = 1;
            + 1 + 5 // uint32 command = 2;
            + 1 + 4 // fixed32 seq = 3;
            + 1 + 5 // uint32 resp_code = 4;
            + 1 + 5 // string biz_code = 5;
            // string resp_msg = 6;
            + 1 + 8; // fixed32 timeout_millis = 7;
    // string extra = 8;


    protected abstract int calcActualBodySize();

    protected abstract boolean encodeBody(EncodeContext context, ByteBuffer dest);

    public final int calcMaxPacketSize() {
        if (maxPacketSize == 0) {
            maxPacketSize = MAX_HEADER_SIZE
                    + (msgBytes == null ? 0 : msgBytes.length)
                    + (extra == null ? 0 : extra.length)
                    + actualBodySize();
        }
        return maxPacketSize;
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
    public final int actualSize() {
        int dumpSize = this.dumpSize;
        if (dumpSize == 0) {
            dumpSize = 4 // length
                    + PbUtil.accurateUnsignedIntSize(IDX_TYPE, packetType) // uint32 packet_type = 1;
                    + PbUtil.accurateUnsignedIntSize(IDX_COMMAND, command) // uint32 command = 2;
                    + PbUtil.accurateFix32Size(IDX_SEQ, seq) // fixed32 seq = 3;
                    + PbUtil.accurateUnsignedIntSize(IDX_RESP_CODE, respCode) // uint32 resp_code = 4;
                    + PbUtil.accurateUnsignedIntSize(IDX_BIZ_CODE, bizCode) // uint32 biz_code = 5;
                    + PbUtil.accurateLengthDelimitedSize(IDX_MSG, msgBytes == null ? 0 : msgBytes.length) // string resp_msg = 6;
                    + PbUtil.accurateFix64Size(IDX_TIMEOUT, timeout) // fixed64 timeout = 7;
                    + PbUtil.accurateLengthDelimitedSize(IDX_EXTRA, extra == null ? 0 : extra.length) // bytes extra = 8;
                    + PbUtil.accurateLengthDelimitedSize(IDX_BODY, actualBodySize()); // bytes body = 15;
            this.dumpSize = dumpSize;
        }
        return dumpSize;
    }

    @Override
    public final boolean encode(EncodeContext context, ByteBuffer buf) {
        int step = context.stage;
        if (step == STATUS_INIT) {
            int totalSize = actualSize();
            int headerSize = totalSize - actualBodySize();
            if (buf.remaining() < headerSize) {
                return false;
            } else {
                buf.putInt(totalSize - 4); //not include total length
                PbUtil.writeUnsignedInt32(buf, IDX_TYPE, packetType);
                PbUtil.writeUnsignedInt32(buf, IDX_COMMAND, command);
                PbUtil.writeFix32(buf, IDX_SEQ, seq);
                PbUtil.writeUnsignedInt32(buf, IDX_RESP_CODE, respCode);
                PbUtil.writeUnsignedInt32(buf, IDX_BIZ_CODE, bizCode);
                PbUtil.writeUTF8(buf, IDX_MSG, msg);
                PbUtil.writeFix64(buf, IDX_TIMEOUT, timeout);
                PbUtil.writeBytes(buf, IDX_EXTRA, extra);
                if (bodySize > 0) {
                    PbUtil.writeLengthDelimitedPrefix(buf, Packet.IDX_BODY, bodySize);
                }
                step = STATUS_HEADER_ENCODE_FINISHED;
            }
        }
        boolean finish = false;
        if (step == STATUS_HEADER_ENCODE_FINISHED) {
            try {
                if (bodySize > 0) {
                    int x = buf.position();
                    finish = encodeBody(context.createOrGetNestedContext(false), buf);
                    x = buf.position() - x;
                    if (finish) {
                        if (bodySize != x + context.pending) {
                            throw new CodecException(this + " body size not match actual encoded size: "
                                    + bodySize + ", " + (x + context.pending));
                        }
                    } else {
                        context.pending += x;
                    }
                } else {
                    finish = true;
                }
            } finally {
                if (finish) {
                    step = STATUS_ENCODE_FINISHED;
                }
            }
        } else {
            throw new NetException("invalid status: " + step);
        }
        context.setStage(step);
        return finish;
    }

    public final void clean() {
        if (cleaned) {
            BugLog.getLog().error("already cleaned {}", this);
            return;
        }
        try {
            doClean();
        } catch (Throwable e) {
            log.error("clean error", e);
        } finally {
            cleaned = true;
        }
    }

    /**
     * may be called in io thread (or other thread).
     */
    protected void doClean() {
    }

    public void prepareRetry() {
        use = false;
        cleaned = false;
    }

    public boolean canRetry() {
        return false;
    }

}

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
package com.github.dtprj.dongting.dtmq.codec;

import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.codec.ParseUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbException;
import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.dtmq.DtMsg;
import com.github.dtprj.dongting.dtmq.MqPullResp;
import com.github.dtprj.dongting.dtmq.MqPullRespDetail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

/**
 * Decodes a pull response, splitting the raw raft log record stream into one {@link DtMsg} per record.
 *
 * @author huangli
 */
public class MqPullRespCallback extends MqPullResp implements PbCallback<MqPullResp> {
    // the fixed header is [totalLen][bizHeaderLen][bodyLen][type/bizType/timestamp][term][prevLogTerm][index][bizKey]
    // future fields are appended after bizKey, before the fixed header crc
    private static final int REC_HEAD_LEN = 12;
    private static final int OFFSET_TIMESTAMP = 12;
    private static final int OFFSET_BIZ_KEY = 36;
    private static final int MIN_FIXED_HEADER_LEN = OFFSET_BIZ_KEY + 8;
    private static final int CRC_LEN = 4;

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final int STATE_HEAD = 0;
    private static final int STATE_FIXED = 1;
    private static final int STATE_FIXED_CRC = 2;
    private static final int STATE_HEADER = 3;
    private static final int STATE_HEADER_CRC = 4;
    private static final int STATE_BODY = 5;
    private static final int STATE_BODY_CRC = 6;

    private final ResultCallback resultCallback = new ResultCallback();

    // current result group and number of records already decoded in it
    private int resultIndex;
    private int seqOffset;
    private int declaredResultsSize = -1;

    private int recordState = STATE_HEAD;
    private byte[] fixedBuf;
    private int fixedPos;
    private int fixedHeaderLen;
    private int bizHeaderLen;
    private int bodyLen;
    private long timestamp;
    private long queueId;
    private byte[] header;
    private int headerPos;
    private byte[] body;
    private int bodyPos;
    private int skip;
    // total length of the records field, used to bound the record lengths declared in its content
    private int recordsFieldLen;

    @Override
    public void readVarNumber(DecodeContext context, int index, long value) {
        if (index == IDX_RESULTS_SIZE && results == null) {
            declaredResultsSize = (int) value;
            results = ParseUtil.createArrayList((int) value);
        }
    }

    @Override
    public void readBytes(DecodeContext context, int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index == IDX_RESULTS) {
            ParseUtil.checkSizeBefore(results, "results");
            MqPullRespDetail result = ParseUtil.parseNested(context, buf, fieldLen, currentPos, resultCallback);
            if (result != null) {
                results.add(result);
            }
        } else if (index == IDX_RECORDS) {
            if (results == null) {
                throw new PbException("results must be encoded before records");
            }
            recordsFieldLen = fieldLen;
            parseRecords(buf);
        }
    }

    @Override
    public MqPullResp getResult(DecodeContext context) {
        if (results != null) {
            ParseUtil.checkCountMatch(declaredResultsSize, results.size(), "results");
            int size = results.size();
            while (resultIndex < size) {
                if (seqOffset < results.get(resultIndex).count) {
                    break;
                }
                resultIndex++;
                seqOffset = 0;
            }
            if (resultIndex != size) {
                throw new PbException("records not complete: result=" + resultIndex + ", seqOffset=" + seqOffset);
            }
        }
        if (recordState != STATE_HEAD || fixedPos != 0) {
            throw new PbException("truncated record");
        }
        if (results == null) {
            results = Collections.emptyList();
        }
        if (records == null) {
            records = Collections.emptyList();
        }
        return this;
    }

    private void parseRecords(ByteBuffer src) {
        while (src.hasRemaining()) {
            switch (recordState) {
                case STATE_HEAD:
                    if (!readRecordHead(src)) {
                        return;
                    }
                    if (recordState != STATE_FIXED) {
                        break;
                    }
                    // fall through
                case STATE_FIXED:
                    if (!readFixedHeader(src)) {
                        return;
                    }
                    // fall through
                case STATE_FIXED_CRC:
                case STATE_HEADER_CRC:
                case STATE_BODY_CRC:
                    if (!skipCrc(src)) {
                        return;
                    }
                    break;
                case STATE_HEADER:
                    if (!readHeader(src)) {
                        return;
                    }
                    break;
                case STATE_BODY:
                    if (!readBody(src)) {
                        return;
                    }
                    break;
                default:
                    throw new PbException("invalid record state: " + recordState);
            }
        }
    }

    private boolean readRecordHead(ByteBuffer src) {
        int start = src.position();
        if (fixedPos == 0 && src.remaining() >= REC_HEAD_LEN) {
            // fast path: the whole fixed header is in this chunk, parse it in place
            beginRecord(src.getInt(start), src.getInt(start + 4), src.getInt(start + 8));
            int need = fixedHeaderLen + CRC_LEN;
            if (src.remaining() >= need) {
                timestamp = src.getLong(start + OFFSET_TIMESTAMP) & 0xFFFF_FFFF_FFFFL;
                queueId = src.getLong(start + OFFSET_BIZ_KEY);
                initPayload();
                src.position(start + fixedHeaderLen + CRC_LEN);
                nextPayloadState();
                return true;
            }
            // the fixed header crosses the chunk boundary, buffer it
            if (fixedBuf == null) {
                fixedBuf = new byte[MIN_FIXED_HEADER_LEN];
            }
            if (fixedBuf.length < fixedHeaderLen) {
                fixedBuf = Arrays.copyOf(fixedBuf, fixedHeaderLen);
            }
            src.get(fixedBuf, 0, REC_HEAD_LEN);
            fixedPos = REC_HEAD_LEN;
            recordState = STATE_FIXED;
            return true;
        }
        if (fixedBuf == null) {
            fixedBuf = new byte[MIN_FIXED_HEADER_LEN];
        }
        int n = Math.min(REC_HEAD_LEN - fixedPos, src.remaining());
        src.get(fixedBuf, fixedPos, n);
        fixedPos += n;
        if (fixedPos < REC_HEAD_LEN) {
            return false;
        }
        beginRecord(DtUtil.JDK_OPS.getInt(fixedBuf, 0), DtUtil.JDK_OPS.getInt(fixedBuf, 4),
                DtUtil.JDK_OPS.getInt(fixedBuf, 8));
        if (fixedBuf.length < fixedHeaderLen) {
            fixedBuf = Arrays.copyOf(fixedBuf, fixedHeaderLen);
        }
        recordState = STATE_FIXED;
        return true;
    }

    private void beginRecord(int totalLen, int bizHeaderLen, int bodyLen) {
        if (totalLen <= 0 || totalLen > recordsFieldLen || bizHeaderLen < 0 || bodyLen <= 0) {
            throw new PbException("bad record header: totalLen=" + totalLen + ", recordsFieldLen="
                    + recordsFieldLen + ", bizHeaderLen=" + bizHeaderLen + ", bodyLen=" + bodyLen);
        }
        long fixedLen = (long) totalLen - 4
                - (bizHeaderLen == 0 ? 0 : (long) bizHeaderLen + 4)
                - bodyLen - 4;
        if (fixedLen < MIN_FIXED_HEADER_LEN) {
            throw new PbException("bad fixed header len: " + fixedLen);
        }
        this.bizHeaderLen = bizHeaderLen;
        this.bodyLen = bodyLen;
        fixedHeaderLen = (int) fixedLen;
    }

    private boolean readFixedHeader(ByteBuffer src) {
        int n = Math.min(fixedHeaderLen - fixedPos, src.remaining());
        src.get(fixedBuf, fixedPos, n);
        fixedPos += n;
        if (fixedPos < fixedHeaderLen) {
            return false;
        }
        timestamp = DtUtil.JDK_OPS.getLong(fixedBuf, OFFSET_TIMESTAMP) & 0xFFFF_FFFF_FFFFL;
        queueId = DtUtil.JDK_OPS.getLong(fixedBuf, OFFSET_BIZ_KEY);
        initPayload();
        recordState = STATE_FIXED_CRC;
        skip = CRC_LEN;
        return true;
    }

    private void initPayload() {
        header = bizHeaderLen == 0 ? EMPTY_BYTES : new byte[bizHeaderLen];
        headerPos = 0;
        body = new byte[bodyLen];
        bodyPos = 0;
    }

    private void nextPayloadState() {
        if (bizHeaderLen > 0) {
            recordState = STATE_HEADER;
        } else {
            recordState = STATE_BODY;
        }
    }

    private boolean skipCrc(ByteBuffer src) {
        int n = Math.min(skip, src.remaining());
        src.position(src.position() + n);
        skip -= n;
        if (skip > 0) {
            return false;
        }
        if (recordState == STATE_FIXED_CRC) {
            nextPayloadState();
        } else if (recordState == STATE_HEADER_CRC) {
            recordState = STATE_BODY;
        } else {
            finishRecord();
        }
        return true;
    }

    private boolean readHeader(ByteBuffer src) {
        int n = Math.min(bizHeaderLen - headerPos, src.remaining());
        src.get(header, headerPos, n);
        headerPos += n;
        if (headerPos < bizHeaderLen) {
            return false;
        }
        recordState = STATE_HEADER_CRC;
        skip = CRC_LEN;
        return true;
    }

    private boolean readBody(ByteBuffer src) {
        int n = Math.min(bodyLen - bodyPos, src.remaining());
        src.get(body, bodyPos, n);
        bodyPos += n;
        if (bodyPos < bodyLen) {
            return false;
        }
        recordState = STATE_BODY_CRC;
        skip = CRC_LEN;
        return true;
    }

    private void finishRecord() {
        while (resultIndex < results.size()
                && seqOffset >= results.get(resultIndex).count) {
            resultIndex++;
            seqOffset = 0;
        }
        if (resultIndex >= results.size()) {
            throw new PbException("record count exceeds results");
        }
        MqPullRespDetail result = results.get(resultIndex);
        if (result.queueId != queueId) {
            throw new PbException("record queueId mismatch: " + queueId + ", " + result.queueId);
        }
        if (records == null) {
            int msgCount = 0;
            for (MqPullRespDetail r : results) {
                msgCount += r.count;
            }
            records = ParseUtil.createArrayList(msgCount);
        }
        records.add(new DtMsg(queueId, header, body, timestamp, result.fromSeq + seqOffset));
        seqOffset++;

        recordState = STATE_HEAD;
        fixedPos = 0;
        fixedHeaderLen = 0;
        bizHeaderLen = 0;
        bodyLen = 0;
        header = null;
        body = null;
        headerPos = 0;
        bodyPos = 0;
    }

    private static final class ResultCallback implements PbCallback<MqPullRespDetail> {
        private long queueId;
        private long fromSeq;
        private int count;
        private boolean truncated;

        @Override
        public void begin(int len) {
            queueId = 0;
            fromSeq = 0;
            count = 0;
            truncated = false;
        }

        @Override
        public void readFix64(DecodeContext context, int index, long value) {
            if (index == MqPullRespDetail.IDX_QUEUE_ID) {
                queueId = value;
            } else if (index == MqPullRespDetail.IDX_FROM_SEQ) {
                fromSeq = value;
            }
        }

        @Override
        public void readVarNumber(DecodeContext context, int index, long value) {
            if (index == MqPullRespDetail.IDX_COUNT) {
                if (value < 0 || value > Integer.MAX_VALUE) {
                    throw new PbException("invalid count: " + value);
                }
                count = (int) value;
            } else if (index == MqPullRespDetail.IDX_TRUNCATED) {
                truncated = value != 0;
            }
        }

        @Override
        public MqPullRespDetail getResult(DecodeContext context) {
            if (queueId <= 0) {
                throw new PbException("invalid queueId: " + queueId);
            }
            if (fromSeq < 0) {
                throw new PbException("invalid fromSeq: " + fromSeq);
            }
            return new MqPullRespDetail(queueId, fromSeq, count, truncated);
        }
    }
}

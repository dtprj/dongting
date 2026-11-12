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
import com.github.dtprj.dongting.dtmq.DtMsg;
import com.github.dtprj.dongting.dtmq.MqPullResp;
import com.github.dtprj.dongting.dtmq.MqPullRespDetail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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

    private static final int REC_HEAD = 0;
    private static final int REC_FIXED = 1;
    private static final int REC_FIXED_CRC = 2;
    private static final int REC_HEADER = 3;
    private static final int REC_HEADER_CRC = 4;
    private static final int REC_BODY = 5;
    private static final int REC_BODY_CRC = 6;

    private final ResultCallback resultCallback = new ResultCallback();

    private int declaredResultsSize;
    private ArrayList<MqPullRespDetail> resultList;
    private ArrayList<DtMsg> recordList;
    // current result group and number of records already decoded in it
    private int resultIndex;
    private int seqOffset;

    private int recordState = REC_HEAD;
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
        if (index == IDX_RESULTS_SIZE) {
            declaredResultsSize = (int) value;
        }
    }

    @Override
    public void readBytes(DecodeContext context, int index, ByteBuffer buf, int fieldLen, int currentPos) {
        if (index == IDX_RESULTS) {
            if (resultList == null) {
                resultList = ParseUtil.createArrayList(declaredResultsSize > 0 ? declaredResultsSize : 4);
            }
            MqPullRespDetail result = ParseUtil.parseNested(context, buf, fieldLen, currentPos, resultCallback);
            if (result != null) {
                resultList.add(result);
            }
        } else if (index == IDX_RECORDS) {
            if (resultList == null) {
                throw new PbException("results must be encoded before records");
            }
            recordsFieldLen = fieldLen;
            parseRecords(buf);
        } else {
            buf.position(buf.limit());
        }
    }

    @Override
    public MqPullResp getResult(DecodeContext context) {
        int size = resultList == null ? 0 : resultList.size();
        if (resultList != null) {
            while (resultIndex < size) {
                if (seqOffset < resultList.get(resultIndex).count) {
                    break;
                }
                resultIndex++;
                seqOffset = 0;
            }
            if (resultIndex != size) {
                throw new PbException("records not complete: result=" + resultIndex + ", seqOffset=" + seqOffset);
            }
        }
        if (recordState != REC_HEAD || fixedPos != 0) {
            throw new PbException("truncated record");
        }
        results = resultList == null ? Collections.emptyList() : resultList;
        records = recordList == null ? Collections.emptyList() : recordList;
        return this;
    }

    private void parseRecords(ByteBuffer src) {
        while (src.hasRemaining()) {
            switch (recordState) {
                case REC_HEAD:
                    if (!readRecordHead(src)) {
                        return;
                    }
                    if (recordState != REC_FIXED) {
                        break;
                    }
                    // fall through
                case REC_FIXED:
                    if (!readFixedHeader(src)) {
                        return;
                    }
                    // fall through
                case REC_FIXED_CRC:
                case REC_HEADER_CRC:
                case REC_BODY_CRC:
                    if (!skipCrc(src)) {
                        return;
                    }
                    break;
                case REC_HEADER:
                    if (!readHeader(src)) {
                        return;
                    }
                    break;
                case REC_BODY:
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
            recordState = REC_FIXED;
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
        beginRecord(getInt(fixedBuf, 0), getInt(fixedBuf, 4), getInt(fixedBuf, 8));
        if (fixedBuf.length < fixedHeaderLen) {
            fixedBuf = Arrays.copyOf(fixedBuf, fixedHeaderLen);
        }
        recordState = REC_FIXED;
        return true;
    }

    private void beginRecord(int totalLen, int bizHeaderLen, int bodyLen) {
        if (totalLen <= 0 || totalLen > recordsFieldLen || bizHeaderLen < 0 || bodyLen < 0) {
            throw new PbException("bad record header: totalLen=" + totalLen + ", recordsFieldLen="
                    + recordsFieldLen + ", bizHeaderLen=" + bizHeaderLen + ", bodyLen=" + bodyLen);
        }
        long fixedLen = (long) totalLen - 4
                - (bizHeaderLen == 0 ? 0 : (long) bizHeaderLen + 4)
                - (bodyLen == 0 ? 0 : (long) bodyLen + 4);
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
        timestamp = getLong(fixedBuf, OFFSET_TIMESTAMP) & 0xFFFF_FFFF_FFFFL;
        queueId = getLong(fixedBuf, OFFSET_BIZ_KEY);
        initPayload();
        recordState = REC_FIXED_CRC;
        skip = CRC_LEN;
        return true;
    }

    private void initPayload() {
        header = bizHeaderLen == 0 ? EMPTY_BYTES : new byte[bizHeaderLen];
        headerPos = 0;
        body = bodyLen == 0 ? EMPTY_BYTES : new byte[bodyLen];
        bodyPos = 0;
    }

    private void nextPayloadState() {
        if (bizHeaderLen > 0) {
            recordState = REC_HEADER;
        } else if (bodyLen > 0) {
            recordState = REC_BODY;
        } else {
            finishRecord();
        }
    }

    private boolean skipCrc(ByteBuffer src) {
        int n = Math.min(skip, src.remaining());
        src.position(src.position() + n);
        skip -= n;
        if (skip > 0) {
            return false;
        }
        if (recordState == REC_FIXED_CRC) {
            if (bizHeaderLen > 0) {
                recordState = REC_HEADER;
            } else if (bodyLen > 0) {
                recordState = REC_BODY;
            } else {
                finishRecord();
            }
        } else if (recordState == REC_HEADER_CRC) {
            if (bodyLen > 0) {
                recordState = REC_BODY;
            } else {
                finishRecord();
            }
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
        recordState = REC_HEADER_CRC;
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
        recordState = REC_BODY_CRC;
        skip = CRC_LEN;
        return true;
    }

    private void finishRecord() {
        while (resultIndex < resultList.size()
                && seqOffset >= resultList.get(resultIndex).count) {
            resultIndex++;
            seqOffset = 0;
        }
        if (resultIndex >= resultList.size()) {
            throw new PbException("record count exceeds results");
        }
        MqPullRespDetail result = resultList.get(resultIndex);
        if (result.queueId != queueId) {
            throw new PbException("record queueId mismatch: " + queueId + ", " + result.queueId);
        }
        if (recordList == null) {
            int expected = 0;
            for (MqPullRespDetail r : resultList) {
                expected += r.count;
                if (expected < 0 || expected > 1024) {
                    expected = 1024;
                    break;
                }
            }
            recordList = new ArrayList<>(expected);
        }
        recordList.add(new DtMsg(queueId, header, body, timestamp, result.fromSeq + seqOffset));
        seqOffset++;

        recordState = REC_HEAD;
        fixedPos = 0;
        fixedHeaderLen = 0;
        bizHeaderLen = 0;
        bodyLen = 0;
        header = null;
        body = null;
        headerPos = 0;
        bodyPos = 0;
    }

    private static int getInt(byte[] src, int offset) {
        return ((src[offset] & 0xFF) << 24)
                | ((src[offset + 1] & 0xFF) << 16)
                | ((src[offset + 2] & 0xFF) << 8)
                | (src[offset + 3] & 0xFF);
    }

    private static long getLong(byte[] src, int offset) {
        return ((long) (src[offset] & 0xFF) << 56)
                | ((long) (src[offset + 1] & 0xFF) << 48)
                | ((long) (src[offset + 2] & 0xFF) << 40)
                | ((long) (src[offset + 3] & 0xFF) << 32)
                | ((long) (src[offset + 4] & 0xFF) << 24)
                | ((long) (src[offset + 5] & 0xFF) << 16)
                | ((long) (src[offset + 6] & 0xFF) << 8)
                | (src[offset + 7] & 0xFF);
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
            MqPullRespDetail result = new MqPullRespDetail();
            result.queueId = queueId;
            result.fromSeq = fromSeq;
            result.count = count;
            result.truncated = truncated;
            return result;
        }
    }
}

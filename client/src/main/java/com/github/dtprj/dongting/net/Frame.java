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

/**
 * @author huangli
 */
public abstract class Frame {
    public static final int IDX_TYPE = 1;
    public static final int IDX_COMMAND = 2;
    public static final int IDX_SEQ = 3;
    public static final int IDX_RESP_CODE = 4;
    public static final int IDX_MSG = 5;
    public static final int IDX_TIMOUT = 6;
    public static final int IDX_EXTRA = 7;
    public static final int IDX_BODY = 15;

    protected int frameType;
    protected int command;
    protected int seq;
    protected int respCode;
    protected String msg;
    protected long timeout;
    protected byte[] extra;

    @Override
    public String toString() {
        return "Frame(type=" + frameType +
                ",cmd=" + command +
                ",seq=" + seq +
                ",respCode=" + respCode +
                ")";
    }


    public int getFrameType() {
        return frameType;
    }

    void setFrameType(int frameType) {
        this.frameType = frameType;
    }

    public int getCommand() {
        return command;
    }

    public void setCommand(int command) {
        this.command = command;
    }

    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

    public int getRespCode() {
        return respCode;
    }

    public void setRespCode(int respCode) {
        this.respCode = respCode;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public byte[] getExtra() {
        return extra;
    }

    public void setExtra(byte[] extra) {
        this.extra = extra;
    }
}

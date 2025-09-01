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
public abstract class Packet {
    public static final int IDX_TYPE = 1;
    public static final int IDX_COMMAND = 2;
    public static final int IDX_SEQ = 3;
    public static final int IDX_RESP_CODE = 4;
    public static final int IDX_BIZ_CODE = 5;
    public static final int IDX_MSG = 6;
    public static final int IDX_TIMEOUT = 7;
    public static final int IDX_EXTRA = 8;
    public static final int IDX_BODY = 15;

    public int packetType;
    public int command;
    public int seq;
    public int respCode;
    public int bizCode;
    public String msg;
    public long timeout;
    public byte[] extra;

    @Override
    public String toString() {
        return "Packet(type=" + packetType +
                ",cmd=" + command +
                ",seq=" + seq +
                ",respCode=" + respCode +
                ",bizCode=" + bizCode +
                ")";
    }
}

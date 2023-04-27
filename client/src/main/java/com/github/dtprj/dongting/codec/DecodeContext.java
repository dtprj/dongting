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
package com.github.dtprj.dongting.codec;

import com.github.dtprj.dongting.buf.RefBufferFactory;
import com.github.dtprj.dongting.net.StringFieldDecoder;

/**
 * @author huangli
 */
public class DecodeContext {
    private RefBufferFactory heapPool;

    private StringFieldDecoder strDecoder;
    private Object status;
    private PbParser pbParser;

    public RefBufferFactory getHeapPool() {
        return heapPool;
    }

    public void setHeapPool(RefBufferFactory heapPool) {
        this.heapPool = heapPool;
    }

    public StringFieldDecoder getStrDecoder() {
        return strDecoder;
    }

    public void setStrDecoder(StringFieldDecoder strDecoder) {
        this.strDecoder = strDecoder;
    }

    public Object getStatus() {
        return status;
    }

    public void setStatus(Object status) {
        this.status = status;
    }

    public PbParser getPbParser() {
        return pbParser;
    }

    public void setPbParser(PbParser pbParser) {
        this.pbParser = pbParser;
    }
}

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

import com.github.dtprj.dongting.pb.PbCallback;
import com.github.dtprj.dongting.pb.PbParser;

import java.nio.ByteBuffer;

/**
 * @author huangli
 */
public abstract class PbZeroCopyDecoder extends Decoder {
    @Override
    public boolean supportHalfPacket() {
        return true;
    }

    @Override
    public Object decode(ProcessContext context, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
        PbParser parser;
        PbCallback callback = null;
        if (start) {
            callback = createCallback(context);
            // TODO reuse parser object
            parser = PbParser.singleParser(callback, bodyLen);
            if (!end) {
                context.setIoDecodeStatus(parser);
            }
        } else {
            parser = (PbParser) context.getIoDecodeStatus();
        }
        parser.parse(buffer);
        if (end) {
            if (callback == null) {
                callback = parser.getCallback();
            }
            return callback.getResult();
        } else {
            return null;
        }
    }

    protected abstract PbCallback createCallback(ProcessContext context);

}

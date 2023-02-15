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
import java.util.function.Function;

/**
 * @author huangli
 */
public class PbZeroCopyDecoder extends Decoder {

    private final Function<ChannelContext, PbCallback> callbackCreator;

    public PbZeroCopyDecoder(Function<ChannelContext, PbCallback> callbackCreator) {
        this.callbackCreator = callbackCreator;
    }

    @Override
    public final boolean supportHalfPacket() {
        return true;
    }

    @Override
    public final boolean decodeInIoThread() {
        return true;
    }

    @Override
    public Object decode(ChannelContext context, ByteBuffer buffer, int bodyLen, boolean start, boolean end) {
        PbParser parser;
        PbCallback callback;
        if (start) {
            parser = context.getIoParser().createOrGetNestedParserSingle(callbackCreator.apply(context), bodyLen);
        } else {
            parser = context.getIoParser().getNestedParser();
        }
        callback = parser.getCallback();
        parser.parse(buffer);
        if (end) {
            return callback.getResult();
        } else {
            return null;
        }
    }

}

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

import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.buf.PoolFactory;
import com.github.dtprj.dongting.codec.DecodeContext;
import com.github.dtprj.dongting.common.NoopPerfCallback;
import com.github.dtprj.dongting.common.PerfCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public abstract class NioConfig {

    public int bizThreads;
    public String name;

    // back pressure config
    public int maxOutRequests;
    public long maxOutBytes;
    public int maxInRequests;
    public long maxInBytes;

    public long selectTimeout = 50; // in milliseconds
    public long cleanInterval = 100; // in milliseconds
    public long nearTimeoutThreshold = 850; // in milliseconds

    public int maxPacketSize = 5 * 1024 * 1024;
    public int maxBodySize = 4 * 1024 * 1024;

    public PoolFactory poolFactory = new DefaultPoolFactory();

    public int readBufferSize = 128 * 1024;

    public PerfCallback perfCallback = NoopPerfCallback.INSTANCE;
    public Supplier<DecodeContext> decodeContextFactory = DecodeContext::new;

    public List<ChannelListener> channelListeners = new ArrayList<>(1);

    public boolean serverHint = true;
}

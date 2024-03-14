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
package com.github.dtprj.dongting.bench.store;

import com.github.dtprj.dongting.net.NioServer;

/**
 * @author huangli
 */
public class RaftLogBenchmark extends RpcBenchmark {

    private final BenchRaftLogProcessor processor = new BenchRaftLogProcessor();

    public static void main(String[] args) throws Exception {
        RpcBenchmark benchmark = new RaftLogBenchmark(1, 1000, 200,
                BenchRaftLogProcessor.COMMAND);
        benchmark.start();
    }

    public RaftLogBenchmark(int threadCount, long testTime, long warmupTime, int cmd) {
        super(threadCount, testTime, warmupTime, cmd);
    }

    @Override
    protected void registerProcessor(NioServer server) {
        server.register(BenchRaftLogProcessor.COMMAND, processor, null);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        processor.shutdown();
    }

}

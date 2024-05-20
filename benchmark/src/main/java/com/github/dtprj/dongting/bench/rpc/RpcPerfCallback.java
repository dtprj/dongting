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
package com.github.dtprj.dongting.bench.rpc;

import com.github.dtprj.dongting.common.PerfCallback;
import io.prometheus.client.SimpleCollector;
import io.prometheus.client.Summary;

import java.lang.reflect.Field;
import java.util.SortedMap;

/**
 * @author huangli
 */
public class RpcPerfCallback extends PerfCallback {

    private static final Summary rpcAcquire = createSummary("rpc_acquire");
    private static final Summary rpcWorkerQueue = createSummary("rpc_worker_queue");
    private static final Summary rpcChannelQueue = createSummary("rpc_channel_queue");

    public RpcPerfCallback(boolean useNanos) {
        super(useNanos);
    }

    @Override
    public boolean accept(int perfType) {
        return true;
    }

    private static Summary createSummary(String name) {
        return Summary.build()
                .name(name)
                .help(name)
                .quantile(0.0, 0.0)
                .quantile(0.99, 0.003)
                .quantile(1.0, 0.0)
                .register();
    }

    @Override
    public void onDuration(int perfType, long costTime, long value) {
        switch (perfType) {
            case PerfCallback.D_RPC_ACQUIRE:
                rpcAcquire.observe(costTime);
                break;
            case PerfCallback.D_RPC_WORKER_QUEUE:
                rpcWorkerQueue.observe(costTime);
                break;
            case PerfCallback.D_RPC_CHANNEL_QUEUE:
                rpcChannelQueue.observe(costTime);
                break;
        }
    }

    @Override
    public void onCount(int perfType, long value) {
    }

    public void printStats() {
        System.out.println("-----------------perf stats----------------");
        print(rpcAcquire);
        print(rpcWorkerQueue);
        print(rpcChannelQueue);
        System.out.println("-------------------------------------------");
    }

    protected void print(Summary summary) {
        Summary.Child.Value value = summary.get();
        if (value.count == 0) {
            return;
        }
        double avg = value.sum / value.count;
        SortedMap<Double, Double> q = value.quantiles;
        if (useNanos) {
            System.out.printf("%s: %,.0f, avg: %,.3fus, p99: %,.3fus, max: %,.3fus, min: %,.3fus\n", getName(summary),
                    value.count, avg / 1000, q.get(0.99) / 1000, q.get(1.0) / 1000, q.get(0.0) / 1000);
        } else {
            System.out.printf("%s: %,.0f, avg: %,.1fms, p99: %,.1fms, max: %,.1fms, min: %,.1fms\n", getName(summary),
                    value.count, avg, q.get(0.99), q.get(1.0), q.get(0.0));
        }
    }

    private String getName(SimpleCollector<?> c) {
        try {
            Field f = c.getClass().getSuperclass().getDeclaredField("fullname");
            f.setAccessible(true);
            return (String) f.get(c);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

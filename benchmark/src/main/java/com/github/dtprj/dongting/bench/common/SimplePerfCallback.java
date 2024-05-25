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
package com.github.dtprj.dongting.bench.common;

import com.github.dtprj.dongting.common.PerfCallback;
import io.prometheus.client.SimpleCollector;
import io.prometheus.client.Summary;

import java.lang.reflect.Field;
import java.util.SortedMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author huangli
 */
public abstract class SimplePerfCallback extends PerfCallback {
    public SimplePerfCallback(boolean useNanos) {
        super(useNanos);
    }

    protected Summary createSummary(String name) {
        return Summary.build()
                .name(name)
                .help(name)
                .quantile(0.0, 0.0)
                .quantile(0.99, 0.003)
                .quantile(1.0, 0.0)
                .register();
    }

    protected void printTime(Summary summary) {
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

    protected void printValue(Summary summary) {
        Summary.Child.Value value = summary.get();
        if (value.count == 0 || value.sum == 0) {
            return;
        }
        double avg = value.sum / value.count;
        SortedMap<Double, Double> q = value.quantiles;
        System.out.printf("%s: %,.0f, avg: %,.1f, p99: %,.1f, max: %,.1f, min: %,.1f\n", getName(summary),
                value.count, avg, q.get(0.99), q.get(1.0), q.get(0.0));
    }

    protected void printCount(String name, LongAdder counter) {
        if (counter.sum() == 0) {
            return;
        }
        System.out.println(name + ": " + counter.sum());
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

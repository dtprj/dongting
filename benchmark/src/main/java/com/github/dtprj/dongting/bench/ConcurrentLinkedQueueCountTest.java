/**
 * Created on 2022/12/30.
 */
package com.github.dtprj.dongting.bench;

import com.github.dtprj.dongting.queue.MpscLinkedQueue;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class ConcurrentLinkedQueueCountTest extends MpscCountBenchBase {
    private final Object data = new Object();

    private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) throws Exception {
        new ConcurrentLinkedQueueCountTest(1, 500_000, 10000).start();
    }

    public ConcurrentLinkedQueueCountTest(int threadCount, long testTime, long warmupTime) {
        super(threadCount, testTime, warmupTime);
    }

    @Override
    public void test(int threadIndex, long startTime) {
        queue.offer(data);
    }

    @Override
    protected boolean poll() {
        return queue.poll() != null;
    }
}

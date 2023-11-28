/**
 * Created on 2022/12/30.
 */
package com.github.dtprj.dongting.bench;

import com.github.dtprj.dongting.queue.MpscLinkedQueue;

/**
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class MpscQueueCountTest extends MpscCountBenchBase {
    private final Object data = new Object();

    private final MpscLinkedQueue<Object> queue = MpscLinkedQueue.newInstance();

    public static void main(String[] args) throws Exception {
        new MpscQueueCountTest(1, 500_000, 10000).start();
    }

    public MpscQueueCountTest(int threadCount, long testTime, long warmupTime) {
        super(threadCount, testTime, warmupTime);
    }

    @Override
    public void test(int threadIndex, long startTime) {
        queue.offer(data);
    }

    @Override
    protected boolean poll() {
        return queue.relaxedPoll() != null;
    }
}

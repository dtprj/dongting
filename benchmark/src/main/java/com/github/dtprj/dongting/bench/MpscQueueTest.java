/**
 * Created on 2022/12/30.
 */
package com.github.dtprj.dongting.bench;

import com.github.dtprj.dongting.queue.MpscLinkedQueue;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class MpscQueueTest extends BenchBase {
    private final Object data = new Object();

    private final MpscLinkedQueue<Object> queue = MpscLinkedQueue.newInstance();

    public static void main(String[] args) throws Exception {
        new MpscQueueTest(1, 5000, 1000).start();
    }

    public MpscQueueTest(int threadCount, long testTime, long warmupTime) {
        super(threadCount, testTime, warmupTime);
    }

    @Override
    public void init() {
        new Thread(this::consumerRun).start();
    }

    private void consumerRun() {
        try {
            AtomicInteger state = this.state;
            MpscLinkedQueue<Object> queue = this.queue;
            int s;
            while ((s = state.getOpaque()) < STATE_BEFORE_SHUTDOWN) {
                if (queue.relaxedPoll() != null) {
                    success(s);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void test(int threadIndex, long startTime, int state) {
        queue.offer(data);
    }
}

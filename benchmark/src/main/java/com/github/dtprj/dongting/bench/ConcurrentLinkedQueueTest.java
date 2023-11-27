/**
 * Created on 2022/12/30.
 */
package com.github.dtprj.dongting.bench;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class ConcurrentLinkedQueueTest extends BenchBase {
    private final Object data = new Object();

    private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) throws Exception {
        new ConcurrentLinkedQueueTest(1, 5000, 1000).start();
    }

    public ConcurrentLinkedQueueTest(int threadCount, long testTime, long warmupTime) {
        super(threadCount, testTime, warmupTime);
    }

    @Override
    public void init() {
        new Thread(this::consumerRun).start();
    }

    private void consumerRun() {
        try {
            AtomicInteger state = this.state;
            ConcurrentLinkedQueue<Object> queue = this.queue;
            int s;
            while ((s = state.getOpaque()) < 2) {
                if (queue.poll() != null) {
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

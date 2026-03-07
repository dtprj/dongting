/**
 * Created on 2022/9/6.
 */
package com.github.dtprj.dongting.buf;

import com.github.dtprj.dongting.common.DtException;
import com.github.dtprj.dongting.common.Timestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.dtprj.dongting.buf.DefaultPoolFactory.DEFAULT_THRESHOLD;
import static com.github.dtprj.dongting.buf.SimpleByteBufferPool.calcTotalSize;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class SimpleByteBufferPoolTest {

    private static final Timestamp TS = new Timestamp();

    private SimpleByteBufferPool pool;

    private void plus(SimpleByteBufferPool pool, long millis) {
        Timestamp ts = pool.getTs();
        Timestamp tsNew = new Timestamp(ts.nanoTime + millis * 1000 * 1000,
                ts.wallClockMillis + millis);
        pool.setTs(tsNew);
    }

    @AfterEach
    public void tearDown() {
        if (pool != null) {
            pool.formatStat();
            pool.cleanAll();
            pool = null;
        }
    }

    private SimpleByteBufferPoolConfig createDefaultConfig(int threshold) {
        return new SimpleByteBufferPoolConfig(TS, false, threshold,
                false, DefaultPoolFactory.DEFAULT_SMALL_SIZE, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT,
                DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT);
    }

    @Test
    public void testConstructor() {
        SimpleByteBufferPoolConfig c1 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, false, null, null, null);
        assertThrows(NullPointerException.class, () -> new SimpleByteBufferPool(c1));

        SimpleByteBufferPoolConfig c2 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, false, new int[]{100}, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT, DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT);
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c2));

        SimpleByteBufferPoolConfig c3 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, false, new int[]{-1}, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT, DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT);
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c3));

        SimpleByteBufferPoolConfig c4 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, false, new int[]{128}, new int[]{-1}, new int[]{2});
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c4));

        SimpleByteBufferPoolConfig c5 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, false, new int[]{128}, new int[]{1}, new int[]{-1});
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c5));

        SimpleByteBufferPoolConfig c6 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, false, new int[]{128}, new int[]{2}, new int[]{4}, -1, 0);
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c6));

        SimpleByteBufferPoolConfig c7 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, false, new int[]{1024, 2048}, new int[]{10, 10}, new int[]{9, 9});
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c7));
    }

    @Test
    public void testBorrow1() {
        pool = new SimpleByteBufferPool(createDefaultConfig(DEFAULT_THRESHOLD));
        ByteBuffer buf1 = pool.borrow(1);
        ByteBuffer buf2 = pool.borrow(1024);
        assertEquals(1, buf1.capacity());
        assertEquals(1024, buf2.capacity());
        assertNotSame(buf1, buf2);
        pool.release(buf1);
        pool.release(buf2);
        ByteBuffer buf3 = pool.borrow(1024);
        assertSame(buf3, buf2);
    }

    @Test
    public void testBorrow2() {
        pool = new SimpleByteBufferPool(createDefaultConfig(DEFAULT_THRESHOLD));
        ByteBuffer buf1 = pool.borrow(1024);
        ByteBuffer buf2 = pool.borrow(1025);
        assertEquals(1024, buf1.capacity());
        assertEquals(2048, buf2.capacity());
    }

    @Test
    public void testBorrow3() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(TS, false, 0, false,
                new int[]{100, 200}, new int[]{10, 10}, new int[]{10, 10});
        pool = new SimpleByteBufferPool(c);
        ByteBuffer buf1 = pool.borrow(300);
        pool.release(buf1);
        assertNotSame(buf1, pool.borrow(300));
    }

    @Test
    public void testRelease() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(TS, false, 0, false,
                new int[]{100, 200}, new int[]{1, 1}, new int[]{2, 2});
        pool = new SimpleByteBufferPool(c);
        ByteBuffer buf1 = pool.borrow(100);
        ByteBuffer buf2 = pool.borrow(100);
        ByteBuffer buf3 = pool.borrow(100);
        pool.release(buf1);
        pool.release(buf2);
        pool.release(buf3);
        assertSame(buf2, pool.borrow(100));
        assertSame(buf1, pool.borrow(100));
    }

    @Test
    public void testThreshold() {
        pool = new SimpleByteBufferPool(createDefaultConfig(2048));
        ByteBuffer buf = pool.borrow(2047);
        assertEquals(2047, buf.capacity());
        pool.release(buf);
        assertNotSame(buf, pool.borrow(2047));

        buf = pool.borrow(2048);
        assertEquals(2048, buf.capacity());
        pool.release(buf);
        assertNotSame(buf, pool.borrow(2048));

        buf = pool.borrow(2049);
        assertEquals(4096, buf.capacity());
        pool.release(buf);
        assertSame(buf, pool.borrow(4096));
    }

    @Test
    public void testThreadSafe() throws Exception {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(null, false, 0, true,
                new int[]{16, 32, 64, 128}, new int[]{1, 1, 1, 1}, new int[]{20, 20, 20, 20}, 1000, 0);
        pool = new SimpleByteBufferPool(c);
        threadSafeTest(pool, 128);
    }

    public static void threadSafeTest(ByteBufferPool pool, int maxCapacity) throws Exception {
        int threadNum = 2;
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        try {
            Runnable runnable = () -> {
                try {
                    for (int i = 0; i < 1000; i++) {
                        Random r = new Random();
                        ByteBuffer bb = pool.borrow(r.nextInt(maxCapacity) + 1);
                        int pos = bb.position();
                        if (pos > 0) {
                            throw new IllegalStateException();
                        } else {
                            bb.position(1);
                        }
                        pool.release(bb);
                        pool.clean();
                    }
                    countDownLatch.countDown();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            };
            for (int i = 0; i < threadNum; i++) {
                executorService.submit(runnable);
            }
            assertTrue(countDownLatch.await(2000, TimeUnit.MILLISECONDS));
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testBadUsage() {
        pool = new SimpleByteBufferPool(createDefaultConfig(DEFAULT_THRESHOLD));
        ByteBuffer buf1 = pool.borrow(400);
        pool.release(buf1);
        assertThrows(DtException.class, () -> pool.release(buf1));
        ByteBuffer buf2 = pool.borrow(400);
        pool.release(buf2);
        buf2.putInt(0, buf2.getInt(0) + 1);
        assertThrows(DtException.class, () -> pool.borrow(400));
        ByteBuffer buf3 = pool.borrow(400);
        // buf2 is dropped
        Assertions.assertNotSame(buf2, buf3);
        pool.release(buf3);
    }

    @Test
    public void testClean() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(TS, false, 0, false,
                new int[]{1024}, new int[]{1}, new int[]{2}, 1000, 0);
        pool = new SimpleByteBufferPool(c);
        ByteBuffer buf1 = pool.borrow(1024);
        ByteBuffer buf2 = pool.borrow(1024);
        pool.release(buf1);
        pool.release(buf2);
        
        plus(pool, 1001);
        pool.clean();
        
        ByteBuffer buf3 = pool.borrow(1024);
        assertSame(buf2, buf3);
        assertNotSame(pool.borrow(1024), buf1);
    }

    public static void main(String[] args) {
        System.out.println("default two level global");
        System.out.printf("max:%,d\nmin:%,d\n\n",
                calcTotalSize(DefaultPoolFactory.DEFAULT_GLOBAL_SIZE, DefaultPoolFactory.DEFAULT_GLOBAL_MAX_COUNT),
                calcTotalSize(DefaultPoolFactory.DEFAULT_GLOBAL_SIZE, DefaultPoolFactory.DEFAULT_GLOBAL_MIN_COUNT));

        System.out.println("default two level small");
        System.out.printf("max:%,d\nmin:%,d\n\n",
                calcTotalSize(DefaultPoolFactory.DEFAULT_SMALL_SIZE, DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT),
                calcTotalSize(DefaultPoolFactory.DEFAULT_SMALL_SIZE, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT));
    }

}

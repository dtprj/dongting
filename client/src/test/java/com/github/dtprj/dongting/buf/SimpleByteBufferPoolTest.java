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

    private ByteBuffer borrowBuf(int size) {
        return pool.borrow(false, size, 0).getBuffer();
    }

    @Test
    public void testBorrow1() {
        pool = new SimpleByteBufferPool(createDefaultConfig(DEFAULT_THRESHOLD));
        ByteBuffer buf1 = borrowBuf(1);
        ByteBuffer buf2 = borrowBuf(1024);
        assertEquals(1, buf1.capacity());
        assertEquals(1024, buf2.capacity());
        assertNotSame(buf1, buf2);
        pool.releaseBuffer(buf1);
        pool.releaseBuffer(buf2);
        ByteBuffer buf3 = borrowBuf(1024);
        assertSame(buf3, buf2);
    }

    @Test
    public void testBorrow2() {
        pool = new SimpleByteBufferPool(createDefaultConfig(DEFAULT_THRESHOLD));
        ByteBuffer buf1 = borrowBuf(1024);
        ByteBuffer buf2 = borrowBuf(1025);
        assertEquals(1024, buf1.capacity());
        assertEquals(2048, buf2.capacity());
    }

    @Test
    public void testBorrow3() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(TS, false, 0, false,
                new int[]{100, 200}, new int[]{10, 10}, new int[]{10, 10});
        pool = new SimpleByteBufferPool(c);
        ByteBuffer buf1 = borrowBuf(300);
        pool.releaseBuffer(buf1);
        assertNotSame(buf1, borrowBuf(300));
    }

    @Test
    public void testRelease() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(TS, false, 0, false,
                new int[]{100, 200}, new int[]{1, 1}, new int[]{2, 2});
        pool = new SimpleByteBufferPool(c);
        ByteBuffer buf1 = borrowBuf(100);
        ByteBuffer buf2 = borrowBuf(100);
        ByteBuffer buf3 = borrowBuf(100);
        pool.releaseBuffer(buf1);
        pool.releaseBuffer(buf2);
        pool.releaseBuffer(buf3);
        assertSame(buf2, borrowBuf(100));
        assertSame(buf1, borrowBuf(100));
    }

    @Test
    public void testThreshold() {
        pool = new SimpleByteBufferPool(createDefaultConfig(2048));
        ByteBuffer buf = borrowBuf(2047);
        assertEquals(2047, buf.capacity());
        pool.releaseBuffer(buf);
        assertNotSame(buf, borrowBuf(2047));

        buf = borrowBuf(2048);
        assertEquals(2048, buf.capacity());
        pool.releaseBuffer(buf);
        assertNotSame(buf, borrowBuf(2048));

        buf = borrowBuf(2049);
        assertEquals(4096, buf.capacity());
        pool.releaseBuffer(buf);
        assertSame(buf, borrowBuf(4096));
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
                        RefBuffer rb = pool.borrow(false, r.nextInt(maxCapacity) + 1, 0);
                        ByteBuffer bb = rb.getBuffer();
                        int pos = bb.position();
                        if (pos > 0) {
                            throw new IllegalStateException();
                        } else {
                            bb.position(1);
                        }
                        rb.release();
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
        ByteBuffer buf1 = borrowBuf(400);
        pool.releaseBuffer(buf1);
        assertThrows(DtException.class, () -> pool.releaseBuffer(buf1));
        ByteBuffer buf2 = borrowBuf(400);
        pool.releaseBuffer(buf2);
        buf2.putInt(0, buf2.getInt(0) + 1);
        assertThrows(DtException.class, () -> borrowBuf(400));
        ByteBuffer buf3 = borrowBuf(400);
        // buf2 is dropped
        Assertions.assertNotSame(buf2, buf3);
        pool.releaseBuffer(buf3);
    }

    @Test
    public void testClean() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(TS, false, 0, false,
                new int[]{1024}, new int[]{1}, new int[]{2}, 1000, 0);
        pool = new SimpleByteBufferPool(c);
        ByteBuffer buf1 = borrowBuf(1024);
        ByteBuffer buf2 = borrowBuf(1024);
        pool.releaseBuffer(buf1);
        pool.releaseBuffer(buf2);

        plus(pool, 1001);
        pool.clean();

        ByteBuffer buf3 = borrowBuf(1024);
        assertSame(buf2, buf3);
        assertNotSame(borrowBuf(1024), buf1);
    }

    @Test
    public void testChainBorrowAndFallback() {
        // small pool: buckets {16, 32}; large pool (thread-safe): buckets {128, 256}
        SimpleByteBufferPool large = new SimpleByteBufferPool(new SimpleByteBufferPoolConfig(
                null, false, 0, true, new int[]{128, 256}, new int[]{1, 2}, new int[]{2, 2}, 1000, 0));
        pool = new SimpleByteBufferPool(new SimpleByteBufferPoolConfig(TS, false, 0, false,
                new int[]{16, 32}, new int[]{1, 2}, new int[]{2, 2}, 1000, 0), large);

        // size within small buckets -> served by small
        RefBuffer b1 = pool.borrow(false, 31, 0);
        RefBuffer b2 = pool.borrow(false, 32, 0);
        assertEquals(32, b1.getBuffer().capacity());
        assertEquals(32, b2.getBuffer().capacity());

        // size beyond small buckets -> delegated to next (large)
        RefBuffer b3 = pool.borrow(false, 33, 0);
        RefBuffer b4 = pool.borrow(false, 128, 0);
        assertEquals(128, b3.getBuffer().capacity());
        assertEquals(128, b4.getBuffer().capacity());

        b1.release();
        b2.release();
        b3.release();
        b4.release();
    }

    @Test
    public void testChainReleaseFallback() {
        // small buckets {16, 32} (maxCount=1 each); large buckets {128} (thread-safe)
        SimpleByteBufferPool large = new SimpleByteBufferPool(new SimpleByteBufferPoolConfig(
                null, false, 0, true, new int[]{128}, new int[]{1}, new int[]{2}, 1000, 0));
        pool = new SimpleByteBufferPool(new SimpleByteBufferPoolConfig(TS, false, 0, false,
                new int[]{16, 32}, new int[]{1, 1}, new int[]{1, 1}, 1000, 0), large);

        // fill the small 32-byte bucket (maxCount=1) then overflow: a 32-byte buffer released while
        // the small bucket is full should fall through to... nowhere (no matching large bucket), so
        // it is released via the chain tail (heap buffer -> dropped, no error).
        RefBuffer b1 = pool.borrow(false, 32, 0);
        RefBuffer b2 = pool.borrow(false, 32, 0); // small allocates a new one (bucket empty after b1)
        assertEquals(32, b1.getBuffer().capacity());
        assertEquals(32, b2.getBuffer().capacity());

        b1.release(); // back to small bucket (now full)
        // releasing b2: small bucket full -> next has no 32-byte bucket -> tail, dropped
        b2.release();
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

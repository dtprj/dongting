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
                DefaultPoolFactory.DEFAULT_SMALL_SIZE, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT,
                DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT);
    }

    @Test
    public void testConstructor() {
        SimpleByteBufferPoolConfig c1 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, null, null, null);
        assertThrows(NullPointerException.class, () -> new SimpleByteBufferPool(c1));

        SimpleByteBufferPoolConfig c2 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, new int[]{100}, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT, DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT);
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c2));

        SimpleByteBufferPoolConfig c3 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, new int[]{-1}, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT, DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT);
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c3));

        SimpleByteBufferPoolConfig c4 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, new int[]{128}, new int[]{-1}, new int[]{2});
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c4));

        SimpleByteBufferPoolConfig c5 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, new int[]{128}, new int[]{1}, new int[]{-1});
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c5));

        SimpleByteBufferPoolConfig c6 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, new int[]{128}, new int[]{2}, new int[]{4}, -1, 0);
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c6));

        SimpleByteBufferPoolConfig c7 = new SimpleByteBufferPoolConfig(TS, false, DEFAULT_THRESHOLD, new int[]{1024, 2048}, new int[]{10, 10}, new int[]{9, 9});
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c7));

        // threshold must not exceed the smallest bucket
        SimpleByteBufferPoolConfig c8 = new SimpleByteBufferPoolConfig(TS, false, 512,
                new int[]{128, 256, 512, 1024}, new int[]{1, 1, 1, 1}, new int[]{2, 2, 2, 2});
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c8));
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
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(TS, false, 0,
                new int[]{100, 200}, new int[]{10, 10}, new int[]{10, 10});
        pool = new SimpleByteBufferPool(c);
        ByteBuffer buf1 = borrowBuf(300);
        pool.releaseBuffer(buf1);
        assertNotSame(buf1, borrowBuf(300));
    }

    @Test
    public void testRelease() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(TS, false, 0,
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
        // threshold equals the smallest bucket: requests <= threshold are not pooled
        pool = new SimpleByteBufferPool(createDefaultConfig(128));
        ByteBuffer buf = borrowBuf(127);
        assertEquals(127, buf.capacity());
        pool.releaseBuffer(buf);
        assertNotSame(buf, borrowBuf(127));

        buf = borrowBuf(128);
        assertEquals(128, buf.capacity());
        pool.releaseBuffer(buf);
        assertNotSame(buf, borrowBuf(128));

        buf = borrowBuf(129);
        assertEquals(256, buf.capacity());
        pool.releaseBuffer(buf);
        assertSame(buf, borrowBuf(256));
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
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(TS, false, 0,
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

    public static void main(String[] args) {
        System.out.println("default global buddy pool");
        System.out.printf("chunkSize:%,d, minChunk:%d, maxChunk:%d\n\n",
                BuddyBufferPoolConfig.DEFAULT_CHUNK_SIZE,
                DefaultPoolFactory.DEFAULT_GLOBAL_MIN_CHUNK_COUNT[0],
                DefaultPoolFactory.DEFAULT_GLOBAL_MAX_CHUNK_COUNT[0]);

        System.out.println("default small pool");
        System.out.printf("max:%,d\nmin:%,d\n\n",
                calcTotalSize(DefaultPoolFactory.DEFAULT_SMALL_SIZE, DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT),
                calcTotalSize(DefaultPoolFactory.DEFAULT_SMALL_SIZE, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT));
    }

}

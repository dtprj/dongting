/**
 * Created on 2022/9/6.
 */
package com.github.dtprj.dongting.buf;

import com.github.dtprj.dongting.common.DtException;
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

    private SimpleByteBufferPool pool;

    @AfterEach
    public void tearDown() {
        if (pool != null) {
            pool.formatStat();
            pool.cleanAll();
            pool = null;
        }
    }

    private SimpleByteBufferPoolConfig createDefaultConfig(int threshold) {
        return new SimpleByteBufferPoolConfig(false, threshold,
                DefaultPoolFactory.DEFAULT_SMALL_SIZE, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT,
                DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT);
    }

    @Test
    public void testConstructor() {
        SimpleByteBufferPoolConfig c1 = new SimpleByteBufferPoolConfig(false, DEFAULT_THRESHOLD, null, null, null);
        assertThrows(NullPointerException.class, () -> new SimpleByteBufferPool(c1));

        SimpleByteBufferPoolConfig c2 = new SimpleByteBufferPoolConfig(false, DEFAULT_THRESHOLD, new int[]{100}, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT, DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT);
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c2));

        SimpleByteBufferPoolConfig c3 = new SimpleByteBufferPoolConfig(false, DEFAULT_THRESHOLD, new int[]{-1}, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT, DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT);
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c3));

        SimpleByteBufferPoolConfig c4 = new SimpleByteBufferPoolConfig(false, DEFAULT_THRESHOLD, new int[]{128}, new int[]{-1}, new int[]{2});
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c4));

        SimpleByteBufferPoolConfig c5 = new SimpleByteBufferPoolConfig(false, DEFAULT_THRESHOLD, new int[]{128}, new int[]{1}, new int[]{-1});
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c5));

        SimpleByteBufferPoolConfig c7 = new SimpleByteBufferPoolConfig(false, DEFAULT_THRESHOLD, new int[]{1024, 2048}, new int[]{10, 10}, new int[]{9, 9});
        assertThrows(IllegalArgumentException.class, () -> new SimpleByteBufferPool(c7));

        // threshold must not exceed the smallest bucket
        SimpleByteBufferPoolConfig c8 = new SimpleByteBufferPoolConfig(false, 512,
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
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(false, 0,
                new int[]{100, 200}, new int[]{10, 10}, new int[]{10, 10});
        pool = new SimpleByteBufferPool(c);
        ByteBuffer buf1 = borrowBuf(300);
        pool.releaseBuffer(buf1);
        assertNotSame(buf1, borrowBuf(300));
    }

    @Test
    public void testRelease() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(false, 0,
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
    public void testShrink() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(false, 0,
                new int[]{1024}, new int[]{1}, new int[]{2}, 0);
        pool = new SimpleByteBufferPool(c);
        ByteBuffer buf1 = borrowBuf(1024);
        ByteBuffer buf2 = borrowBuf(1024);
        pool.releaseBuffer(buf1);
        pool.releaseBuffer(buf2);

        // no borrow this period: minSize==size, shrink half (2 -> 1)
        pool.shrink();

        ByteBuffer buf3 = borrowBuf(1024);
        assertSame(buf2, buf3);
        assertNotSame(borrowBuf(1024), buf1);
    }

    @Test
    public void testShrinkToMinGradually() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(false, 0,
                new int[]{1024}, new int[]{1}, new int[]{8}, 0);
        pool = new SimpleByteBufferPool(c);
        ByteBuffer[] bufs = new ByteBuffer[8];
        for (int i = 0; i < 8; i++) {
            bufs[i] = borrowBuf(1024);
        }
        for (ByteBuffer buf : bufs) {
            pool.releaseBuffer(buf);
        }
        // stack now holds 8 buffers, none borrowed this period.
        // each shrink halves the untouched portion, converging to minCount=1.
        int prev = countPooled(1024);
        for (int i = 0; i < 10; i++) {
            pool.shrink();
            int now = countPooled(1024);
            assertTrue(now <= prev);
            prev = now;
        }
        assertEquals(1, countPooled(1024));
    }

    @Test
    public void testShrinkKeepUsedPortion() {
        SimpleByteBufferPoolConfig c = new SimpleByteBufferPoolConfig(false, 0,
                new int[]{1024}, new int[]{0}, new int[]{8}, 0);
        pool = new SimpleByteBufferPool(c);
        ByteBuffer[] bufs = new ByteBuffer[8];
        for (int i = 0; i < 8; i++) {
            bufs[i] = borrowBuf(1024);
        }
        for (ByteBuffer buf : bufs) {
            pool.releaseBuffer(buf);
        }
        // borrow 5 of them this period: only bottom 3 are untouched
        ByteBuffer b0 = borrowBuf(1024);
        ByteBuffer b1 = borrowBuf(1024);
        ByteBuffer b2 = borrowBuf(1024);
        ByteBuffer b3 = borrowBuf(1024);
        ByteBuffer b4 = borrowBuf(1024);
        // stack size dropped to 3, so periodMinStackSize==3
        pool.shrink();
        // untouched=3, shrink floor(3/2)=1, target=3-1=2; borrowed buffers not in stack
        assertEquals(2, countPooled(1024));
        pool.releaseBuffer(b0);
        pool.releaseBuffer(b1);
        pool.releaseBuffer(b2);
        pool.releaseBuffer(b3);
        pool.releaseBuffer(b4);
    }

    private int countPooled(int size) {
        int idx = 0;
        for (; idx < pool.bufSizes.length; idx++) {
            if (pool.bufSizes[idx] == size) {
                break;
            }
        }
        return pool.pools[idx].bufferStack.size();
    }

    public static void main(String[] args) {
        System.out.println("default small pool");
        System.out.printf("max:%,d\nmin:%,d\n",
                calcTotalSize(DefaultPoolFactory.DEFAULT_SMALL_SIZE, DefaultPoolFactory.DEFAULT_SMALL_MAX_COUNT),
                calcTotalSize(DefaultPoolFactory.DEFAULT_SMALL_SIZE, DefaultPoolFactory.DEFAULT_SMALL_MIN_COUNT));
    }

}

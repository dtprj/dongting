/**
 * Created on 2022/9/6.
 */
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.buf.ByteBufferPool;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class ByteBufferPoolTest {

    @Test
    public void testConstructor() {
        assertThrows(NullPointerException.class, () -> new ByteBufferPool(false, new int[]{1024, 2048},
                new int[]{10, 10}, null, 1000));
        assertThrows(IllegalArgumentException.class, () -> new ByteBufferPool(false, new int[]{1024, 2048},
                new int[]{10, 10}, new int[]{10}, 1000));

        assertThrows(IllegalArgumentException.class, () -> new ByteBufferPool(false, new int[]{-1},
                new int[]{10}, new int[]{10}, 1000));
        assertThrows(IllegalArgumentException.class, () -> new ByteBufferPool(false, new int[]{1024},
                new int[]{-1}, new int[]{10}, 1000));
        assertThrows(IllegalArgumentException.class, () -> new ByteBufferPool(false, new int[]{1024},
                new int[]{10}, new int[]{-1}, 1000));
        assertThrows(IllegalArgumentException.class, () -> new ByteBufferPool(false, new int[]{1024},
                new int[]{10}, new int[]{10}, -1));

        assertThrows(IllegalArgumentException.class, () -> new ByteBufferPool(false, new int[]{1024, 2048},
                new int[]{10, 10}, new int[]{9, 9}, 1000));
    }

    @Test
    public void testBorrow1() {
        ByteBufferPool pool = new ByteBufferPool(false);
        ByteBuffer buf1 = pool.borrow(1);
        ByteBuffer buf2 = pool.borrow(2);
        assertEquals(1024, buf1.capacity());
        assertEquals(1024, buf2.capacity());
        assertNotSame(buf1, buf2);
        pool.release(buf1, System.nanoTime());
        pool.release(buf2, System.nanoTime());
        ByteBuffer buf3 = pool.borrow(1024);
        assertSame(buf3, buf2);
    }

    @Test
    public void testBorrow2() {
        ByteBufferPool pool = new ByteBufferPool(false);
        ByteBuffer buf1 = pool.borrow(1024);
        ByteBuffer buf2 = pool.borrow(1025);
        assertEquals(1024, buf1.capacity());
        assertEquals(2048, buf2.capacity());
    }

    @Test
    public void testBorrow3() {
        ByteBufferPool pool = new ByteBufferPool(false, new int[]{1024, 2048},
                new int[]{10, 10}, new int[]{10, 10}, 1000);
        ByteBuffer buf1 = pool.borrow(4000);
        pool.release(buf1, System.nanoTime());
        assertNotSame(buf1, pool.borrow(4000));
    }

    @Test
    public void testRelease() {
        ByteBufferPool pool = new ByteBufferPool(false, new int[]{1024, 2048},
                new int[]{1, 1}, new int[]{2, 2}, 1000);
        ByteBuffer buf1 = pool.borrow(1024);
        ByteBuffer buf2 = pool.borrow(1024);
        ByteBuffer buf3 = pool.borrow(1024);
        pool.release(buf1, System.nanoTime());
        pool.release(buf2, System.nanoTime());
        pool.release(buf3, System.nanoTime());
        assertSame(buf2, pool.borrow(1024));
        assertSame(buf1, pool.borrow(1024));
    }

    @Test
    public void testClean1() {
        ByteBufferPool pool = new ByteBufferPool(false, new int[]{1024, 2048},
                new int[]{1, 1}, new int[]{2, 2}, 1000);
        ByteBuffer buf1 = pool.borrow(1024);
        ByteBuffer buf2 = pool.borrow(1024);
        long time = System.nanoTime();
        pool.release(buf1, time);
        pool.release(buf2, time);

        // not clean
        pool.clean(time + 500L * 1000 * 1000);
        ByteBuffer buf3 = pool.borrow(1024);
        ByteBuffer buf4 = pool.borrow(1024);
        assertSame(buf2, buf3);
        assertSame(buf1, buf4);
        pool.release(buf1, time);
        pool.release(buf2, time);
    }

    @Test
    public void testClean2() {
        ByteBufferPool pool = new ByteBufferPool(false, new int[]{1024, 2048},
                new int[]{1, 1}, new int[]{3, 3}, 1000);
        ByteBuffer buf1 = pool.borrow(1024);
        ByteBuffer buf2 = pool.borrow(1024);
        ByteBuffer buf3 = pool.borrow(1024);
        long time = System.nanoTime();
        pool.release(buf1, time);
        pool.release(buf2, time);
        pool.release(buf3, time);

        //clean 2 buffer
        for (int i = 0; i < 5; i++) {
            pool.clean(time + 1001L * 1000 * 1000);
            ByteBuffer buf4 = pool.borrow(1024);
            ByteBuffer buf5 = pool.borrow(1024);
            ByteBuffer buf6 = pool.borrow(1024);
            assertSame(buf3, buf4);
            assertTrue(buf5 != buf1 && buf5 != buf2 && buf5 != buf3);
            assertTrue(buf6 != buf1 && buf6 != buf2 && buf6 != buf3);
            buf1 = buf4;
            buf2 = buf5;
            buf3 = buf6;
            pool.release(buf1, time);
            pool.release(buf2, time);
            pool.release(buf3, time);
        }
    }

    @Test
    public void testClean3() {
        ByteBufferPool pool = new ByteBufferPool(false, new int[]{1024, 2048},
                new int[]{1, 1}, new int[]{2, 2}, 1000);
        ByteBuffer buf1 = pool.borrow(2048);
        ByteBuffer buf2 = pool.borrow(2048);
        long time = System.nanoTime();
        pool.release(buf1, time);
        pool.release(buf2, time);

        //clean 2 buffer
        for (int i = 0; i < 5; i++) {
            pool.clean(time + 1001L * 1000 * 1000);
            ByteBuffer buf3 = pool.borrow(2048);
            ByteBuffer buf4 = pool.borrow(2048);
            assertSame(buf2, buf3);
            assertTrue(buf4 != buf1 && buf4 != buf2);
            buf1 = buf3;
            buf2 = buf4;
            pool.release(buf1, time);
            pool.release(buf2, time);
        }
    }

    @Test
    public void testClean4() {
        ByteBufferPool pool = new ByteBufferPool(false, new int[]{1024, 2048},
                new int[]{0, 0}, new int[]{2, 2}, 1000);
        ByteBuffer buf1 = pool.borrow(2048);
        ByteBuffer buf2 = pool.borrow(2048);
        long time = System.nanoTime();
        pool.release(buf1, time);
        pool.release(buf2, time);

        //clean 2 buffer
        for (int i = 0; i < 5; i++) {
            pool.clean(time + 1001L * 1000 * 1000);
            ByteBuffer buf3 = pool.borrow(2048);
            ByteBuffer buf4 = pool.borrow(2048);
            assertTrue(buf3 != buf1 && buf3 != buf2);
            assertTrue(buf4 != buf1 && buf4 != buf2);
            buf1 = buf3;
            buf2 = buf4;
            pool.release(buf1, time);
            pool.release(buf2, time);
        }
    }

    public static void main(String[] args) {
        int[] bufSize = ByteBufferPool.DEFAULT_BUF_SIZE;
        int[] maxCount = ByteBufferPool.DEFAULT_MAX_COUNT;
        int[] minCount = ByteBufferPool.DEFAULT_MIN_COUNT;
        long totalMax = 0;
        long totalMin = 0;
        for (int i = 0; i < bufSize.length; i++) {
            totalMax += bufSize[i] * maxCount[i];
            totalMin += bufSize[i] * minCount[i];
        }
        System.out.printf("max:%,d\nmin:%,d", totalMax, totalMin);
    }
}

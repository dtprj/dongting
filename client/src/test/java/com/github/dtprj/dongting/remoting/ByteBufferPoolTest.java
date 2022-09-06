/**
 * Created on 2022/9/6.
 */
package com.github.dtprj.dongting.remoting;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

/**
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class ByteBufferPoolTest {
    @Test
    public void test() {
        ByteBufferPool pool = new ByteBufferPool(false);
        ByteBuffer buf1 = pool.borrow(1);
        ByteBuffer buf2 = pool.borrow(2);
        Assertions.assertEquals(1024, buf1.capacity());
        Assertions.assertEquals(1024, buf2.capacity());
        Assertions.assertNotSame(buf1, buf2);
        pool.release(buf1, System.nanoTime());
        pool.release(buf2, System.nanoTime());
        ByteBuffer buf3 = pool.borrow(1024);
        Assertions.assertSame(buf3, buf2);
    }
}

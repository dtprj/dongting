/**
 * Created on 2022/10/28.
 */
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.buf.DefaultPoolFactory;
import com.github.dtprj.dongting.codec.DtFrame;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.common.DtThread;
import com.github.dtprj.dongting.common.Timestamp;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class FramePbTest {

    @Test
    public void testInDtThread() throws Exception {
        CompletableFuture<Void> f = new CompletableFuture<>();
        DtThread dtThread = new DtThread(() -> {
            try {
                test();
                f.complete(null);
            } catch (Throwable e) {
                f.completeExceptionally(e);
            }
        }, "dtThread");
        dtThread.start();
        f.get();
    }

    private void test() throws Exception {
        test0(0, 0, 0, 0, "1", new byte[]{1}, 0, 0);
        test0(1, 1, 1, 1, "123", new byte[]{1, 5}, 1, 1);
        test0(1000, 1000, 1000, 1000, "123", null, 10000, 1000);
        test0(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, "汉字", null, Long.MAX_VALUE, 1000);
        test0(-1, -1, -1, -1, "123", null, -1, 1000);
        test0(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, "123", null, Long.MIN_VALUE, 1000);

        char[] cs = new char[2000];
        Arrays.fill(cs, 'a');
        byte[] extra = new byte[2000];
        new Random().nextBytes(extra);
        test0(1000, 1000, 1000, 1000, new String(cs), null, 1000, 1000);
    }

    private void test0(int frameType, int command, int seq, int respCode, String msg, byte[] extra,
                       long timeout, int bodySize) throws Exception {
        testEncode0(frameType, command, seq, respCode, msg, extra, timeout, bodySize);
        testDecode0(frameType, command, seq, respCode, msg, extra, timeout, bodySize);
    }

    private void testEncode0(int frameType, int command, int seq, int respCode, String msg, byte[] extra,
                             long timeout, int bodySize) throws Exception {
        byte[] bs = new byte[bodySize];
        new Random().nextBytes(bs);
        ByteBufferWriteFrame f = new ByteBufferWriteFrame(ByteBuffer.wrap(bs));
        f.setFrameType(frameType);
        f.setCommand(command);
        f.setSeq(seq);
        f.setRespCode(respCode);
        f.setMsg(msg);
        f.setTimeout(timeout);
        f.setExtra(extra);
        ByteBuffer buf = ByteBuffer.allocate(f.actualSize());
        f.encode(new EncodeContext(null), buf);
        buf.flip();
        buf.position(4);
        DtFrame.Frame pbf = DtFrame.Frame.parseFrom(buf);
        assertEquals(frameType, pbf.getFrameType());
        assertEquals(command, pbf.getCommand());
        assertEquals(seq, pbf.getSeq());
        assertEquals(respCode, pbf.getRespCode());
        assertEquals(msg, pbf.getRespMsg());
        assertEquals(timeout, pbf.getTimeout());
        if (extra != null) {
            assertArrayEquals(extra, pbf.getExtra().toByteArray());
        } else {
            assertEquals(0, pbf.getExtra().size());
        }
        assertArrayEquals(bs, pbf.getBody().toByteArray());
    }

    private void testDecode0(int frameType, int command, int seq, int respCode,
                             String msg, byte[] extra, long timeout, int bodySize) throws IOException {
        byte[] bs = new byte[bodySize];
        new Random().nextBytes(bs);
        DtFrame.Frame.Builder builder = DtFrame.Frame.newBuilder()
                .setFrameType(frameType)
                .setCommand(command)
                .setSeq(seq)
                .setRespCode(respCode)
                .setRespMsg(msg)
                .setTimeout(timeout)
                .setBody(ByteString.copyFrom(bs));
        if (extra != null) {
            builder.setExtra(ByteString.copyFrom(extra));
        }
        byte[] encodeBytes = builder.build().toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(encodeBytes.length + 4);
        buf.putInt(encodeBytes.length);
        buf.put(encodeBytes);
        buf.flip();

        WorkerStatus workerStatus = new WorkerStatus();
        workerStatus.setHeapPool(new DefaultPoolFactory().createPool(new Timestamp(), false));

        DtChannel dtc = new DtChannel(new NioStatus(null), workerStatus,
                new NioClientConfig(), SocketChannel.open(), 0) {

            @Override
            public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
                if (index == Frame.IDX_BODY) {
                    byte[] readBytes = new byte[fieldLen];
                    buf.get(readBytes);
                    assertArrayEquals(bs, readBytes);
                    return true;
                } else {
                    return super.readBytes(index, buf, fieldLen, currentPos);
                }
            }
        };
        buf.order(ByteOrder.LITTLE_ENDIAN);
        dtc.getParser().parse(buf);
        assertEquals(frameType, dtc.getFrame().getFrameType());
        assertEquals(command, dtc.getFrame().getCommand());
        assertEquals(seq, dtc.getFrame().getSeq());
        assertEquals(respCode, dtc.getFrame().getRespCode());
        assertEquals(msg, dtc.getFrame().getMsg());
        assertArrayEquals(extra, dtc.getFrame().getExtra());
        assertEquals(timeout, dtc.getFrame().getTimeout());
    }
}

/**
 * Created on 2022/10/28.
 */
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.buf.TwoLevelPool;
import com.github.dtprj.dongting.codec.DtFrame;
import com.github.dtprj.dongting.common.Timestamp;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class FramePbTest {

    @Test
    public void test() throws Exception {
        test0(0, 0, 0, 0, "1", 0, 0);
        test0(1, 1, 1, 1, "123", 1, 1);
        test0(1000, 1000, 1000, 1000, "123", 10000, 1000);
        test0(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, "汉字", Long.MAX_VALUE, 1000);
        test0(-1, -1, -1, -1, "123", -1, 1000);
        test0(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, "123", Long.MIN_VALUE, 1000);

        char[] cs = new char[2000];
        Arrays.fill(cs, 'a');
        test0(1000, 1000, 1000, 1000, new String(cs), 1000, 1000);
    }

    private void test0(int frameType, int command, int seq, int respCode, String msg, long timeout, int bodySize) throws Exception {
        testEncode0(frameType, command, seq, respCode, msg, timeout, bodySize);
        testDecode0(frameType, command, seq, respCode, msg, timeout, bodySize);
    }

    private void testEncode0(int frameType, int command, int seq, int respCode, String msg,
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
        ByteBuffer buf = ByteBuffer.allocate(f.actualSize(null, f));
        f.encode(null, buf, f);
        buf.flip();
        buf.position(4);
        DtFrame.Frame pbf = DtFrame.Frame.parseFrom(buf);
        assertEquals(frameType, pbf.getFrameType());
        assertEquals(command, pbf.getCommand());
        assertEquals(seq, pbf.getSeq());
        assertEquals(respCode, pbf.getRespCode());
        assertEquals(msg, pbf.getRespMsg());
        assertEquals(timeout, pbf.getTimeout());
        assertArrayEquals(bs, pbf.getBody().toByteArray());
    }

    private void testDecode0(int frameType, int command, int seq, int respCode,
                             String msg, long timeout, int bodySize) throws IOException {
        byte[] bs = new byte[bodySize];
        new Random().nextBytes(bs);
        DtFrame.Frame pbf = DtFrame.Frame.newBuilder()
                .setFrameType(frameType)
                .setCommand(command)
                .setSeq(seq)
                .setRespCode(respCode)
                .setRespMsg(msg)
                .setTimeout(timeout)
                .setBody(ByteString.copyFrom(bs))
                .build();
        byte[] encodeBytes = pbf.toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(encodeBytes.length + 4);
        buf.putInt(encodeBytes.length);
        buf.put(encodeBytes);
        buf.flip();

        WorkerStatus workerStatus = new WorkerStatus();
        workerStatus.setHeapPool(TwoLevelPool.getDefaultFactory().apply(new Timestamp(), false));

        DtChannel dtc = new DtChannel(new NioStatus(null), workerStatus,
                new NioClientConfig(), SocketChannel.open(), 0) {
            @Override
            public void end(boolean success) {
            }

            @Override
            public boolean readBytes(int index, ByteBuffer buf, int fieldLen, boolean start, boolean end) {
                if (index == Frame.IDX_BODY) {
                    byte[] readBytes = new byte[fieldLen];
                    buf.get(readBytes);
                    assertArrayEquals(bs, readBytes);
                    return true;
                } else {
                    return super.readBytes(index, buf, fieldLen, start, end);
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
        assertEquals(timeout, dtc.getFrame().getTimeout());
    }
}

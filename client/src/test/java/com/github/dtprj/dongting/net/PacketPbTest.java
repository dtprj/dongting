/**
 * Created on 2022/10/28.
 */
package com.github.dtprj.dongting.net;

import com.github.dtprj.dongting.codec.CodecTestUtil;
import com.github.dtprj.dongting.codec.DtPacket;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.common.Timestamp;
import com.github.dtprj.dongting.java8.Java8NioStatus;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author <a href="mailto:areyouok@gmail.com">huangli</a>
 */
public class PacketPbTest {

    @Test
    public void testInDtThread() throws Exception {
        test();
    }

    private void test() throws Exception {
        test0(1, 0, 0, 0, 1, "1", new byte[]{1}, 0, 0);
        test0(2, 1, 1, 1, 0, "123", new byte[]{1, 5}, 1, 1);
        test0(1000, 1000, 1000, 1000, 2000, "123", null, 10000, 1000);
        test0(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, "汉字", null, Long.MAX_VALUE, 1000);
        test0(-1, -1, -1, -1, -1, "123", null, -1, 1000);
        test0(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, "123", null, Long.MIN_VALUE, 1000);

        char[] cs = new char[2000];
        Arrays.fill(cs, 'a');
        byte[] extra = new byte[2000];
        new Random().nextBytes(extra);
        test0(1000, 1000, 1000, 1000, 2000, new String(cs), null, 1000, 1000);
    }

    private void test0(int packetType, int command, int seq, int respCode, int bizCode, String msg, byte[] extra,
                       long timeout, int bodySize) throws Exception {
        testEncode0(packetType, command, seq, respCode, bizCode, msg, extra, timeout, bodySize);
        testDecode0(packetType, command, seq, respCode, bizCode, msg, extra, timeout, bodySize);
    }

    private void testEncode0(int packetType, int command, int seq, int respCode, int bizCode, String msg, byte[] extra,
                             long timeout, int bodySize) throws Exception {
        byte[] bs = new byte[bodySize];
        new Random().nextBytes(bs);
        ByteBufferWritePacket f = new ByteBufferWritePacket(ByteBuffer.wrap(bs));
        f.packetType = packetType;
        f.command = command;
        f.seq = seq;
        f.respCode = respCode;
        f.bizCode = bizCode;
        f.msg = msg;
        f.timeout = timeout;
        f.extra = extra;
        ByteBuffer buf = ByteBuffer.allocate(f.actualSize());
        f.encode(new EncodeContext(null), buf);
        buf.flip();
        buf.position(4);
        DtPacket.Packet pbf = DtPacket.Packet.parseFrom(buf);
        assertEquals(packetType, pbf.getPacketType());
        assertEquals(command, pbf.getCommand());
        assertEquals(seq, pbf.getSeq());
        assertEquals(respCode, pbf.getRespCode());
        assertEquals(bizCode, pbf.getBizCode());
        assertEquals(msg, pbf.getRespMsg());
        assertEquals(timeout, pbf.getTimeout());
        if (extra != null) {
            assertArrayEquals(extra, pbf.getExtra().toByteArray());
        } else {
            assertEquals(0, pbf.getExtra().size());
        }
        assertArrayEquals(bs, pbf.getBody().toByteArray());
    }

    private void testDecode0(int packetType, int command, int seq, int respCode, int bizCode,
                             String msg, byte[] extra, long timeout, int bodySize) throws IOException {
        byte[] bs = new byte[bodySize];
        new Random().nextBytes(bs);
        DtPacket.Packet.Builder builder = DtPacket.Packet.newBuilder()
                .setPacketType(packetType)
                .setCommand(command)
                .setSeq(seq)
                .setRespCode(respCode)
                .setBizCode(bizCode)
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

        WorkerStatus workerStatus = new WorkerStatus(null, null,
                null, CodecTestUtil.createContext().getHeapPool(), new Timestamp(), 1000);

        SocketChannel sc = SocketChannel.open();
        sc.bind(new InetSocketAddress(19344));
        DtChannelImpl dtc;
        try {
            dtc = new DtChannelImpl(new Java8NioStatus(), workerStatus,
                    new NioClientConfig(), null, sc, 0) {

                @Override
                public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
                    if (index == Packet.IDX_BODY) {
                        byte[] readBytes = new byte[fieldLen];
                        buf.get(readBytes);
                        assertArrayEquals(bs, readBytes);
                        return true;
                    } else {
                        return super.readBytes(index, buf, fieldLen, currentPos);
                    }
                }

                @Override
                protected void writeErrorInIoThread(Packet req, int code, String msg) {
                }

                @Override
                protected boolean end(boolean success) {
                    return success;
                }
            };
        } finally {
            sc.close();
        }
        dtc.getParser().parse(buf);
        assertEquals(packetType, dtc.getPacket().packetType);
        assertEquals(command, dtc.getPacket().command);
        assertEquals(seq, dtc.getPacket().seq);
        assertEquals(respCode, dtc.getPacket().respCode);
        assertEquals(bizCode, dtc.getPacket().bizCode);
        assertEquals(msg, dtc.getPacket().msg);
        assertArrayEquals(extra, dtc.getPacket().extra);
        assertEquals(timeout, dtc.getPacket().timeout);
    }
}

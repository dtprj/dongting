/*
 * Copyright The Dongting Project
 *
 * The Dongting Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.github.dtprj.dongting.raft.store;

import com.github.dtprj.dongting.raft.server.LogItem;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
public class LogHeaderTest {
    @Test
    public void testWriteAndRead() {
        LogItem item = new LogItem();
        item.setType(1);
        item.setBizType(2);
        item.setTerm(1000);
        item.setPrevLogTerm(2000);
        item.setIndex(3000);
        item.setTimestamp(Long.MAX_VALUE);
        ByteBuffer buf = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE);
        CRC32C crc32C = new CRC32C();
        item.setActualHeaderSize(200);
        item.setActualBodySize(300);
        LogHeader.writeHeader(crc32C, buf, item);

        buf.clear();
        LogHeader header = new LogHeader();
        header.read(buf);
        assertEquals(1, header.type);
        assertEquals(2, header.bizType);
        assertEquals(1000, header.term);
        assertEquals(2000, header.prevLogTerm);
        assertEquals(3000, header.index);
        assertEquals(Long.MAX_VALUE, header.timestamp);

        assertEquals(header.totalLen, LogHeader.computeTotalLen(200, 300));
        assertEquals(200, header.bizHeaderLen);
        assertEquals(300, header.bodyLen);
        assertEquals(header.expectCrc, header.headerCrc);
    }

    @Test
    public void testWriteEnd() {
        ByteBuffer buf = ByteBuffer.allocate(LogHeader.ITEM_HEADER_SIZE);
        CRC32C crc32C = new CRC32C();
        LogHeader.writeEndHeader(crc32C, buf);

        buf.clear();
        LogHeader header = new LogHeader();
        header.read(buf);
        assertEquals(0xF19A7BCB, header.totalLen);
        assertEquals(header.expectCrc, header.headerCrc);
    }
}

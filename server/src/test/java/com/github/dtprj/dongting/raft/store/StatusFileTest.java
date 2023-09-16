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

import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author huangli
 */
public class StatusFileTest {
    @Test
    public void testUpdateAndInit() throws Exception {
        File dir = TestDir.createTestDir(StatusFileTest.class.getSimpleName());
        File file = new File(dir, "status");
        StatusFile statusFile = new StatusFile(file, MockExecutors.ioExecutor());
        statusFile.init();
        statusFile.getProperties().setProperty("1", "100");
        statusFile.getProperties().setProperty("2", "200");
        statusFile.update(true).get();
        statusFile.close();

        statusFile = new StatusFile(file, MockExecutors.ioExecutor());
        statusFile.init();
        assertEquals("100", statusFile.getProperties().getProperty("1"));
        assertEquals("200", statusFile.getProperties().getProperty("2"));
        statusFile.getProperties().setProperty("3", "300");
        statusFile.update(true).get();
        statusFile.close();

        statusFile = new StatusFile(file, MockExecutors.ioExecutor());
        statusFile.init();
        assertEquals("100", statusFile.getProperties().getProperty("1"));
        assertEquals("200", statusFile.getProperties().getProperty("2"));
        assertEquals("300", statusFile.getProperties().getProperty("3"));
        statusFile.close();
    }

    @Test
    public void testChecksumError() throws Exception {
        File dir = TestDir.createTestDir(StatusFileTest.class.getSimpleName());
        File file = new File(dir, "status");
        StatusFile statusFile = new StatusFile(file, MockExecutors.ioExecutor());
        statusFile.init();
        statusFile.getProperties().setProperty("1", "100");
        statusFile.getProperties().setProperty("2", "200");
        statusFile.update(true).get();
        statusFile.close();

        FileInputStream in = new FileInputStream(file);
        byte[] bs = in.readAllBytes();
        in.close();

        // shift crc value
        byte b0 = bs[0];
        for (int i = 0; i < 7; i++) {
            bs[i] = bs[i + 1];
        }
        bs[7] = b0;

        FileOutputStream fos = new FileOutputStream(file);
        fos.write(bs);
        fos.close();
        StatusFile sf2 = new StatusFile(file, MockExecutors.ioExecutor());
        assertThrows(ChecksumException.class, sf2::init);
    }

    @Test
    public void testFileLengthError() throws Exception {
        File dir = TestDir.createTestDir(StatusFileTest.class.getSimpleName());
        File file = new File(dir, "status");
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(1);
        raf.close();
        StatusFile statusFile = new StatusFile(file, MockExecutors.ioExecutor());
        assertThrows(RaftException.class, statusFile::init);
    }
}

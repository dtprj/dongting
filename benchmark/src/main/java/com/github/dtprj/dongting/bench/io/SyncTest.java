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
package com.github.dtprj.dongting.bench.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Random;

/**
 * @author huangli
 */
public class SyncTest {
    private static final int SIZE = 4096;
    private static final int WARMUP_COUNT = 100;
    private static final int COUNT = 500;

    private static final byte[] DATA = new byte[SIZE];

    public static void main(String[] args) throws Exception {
        new Random().nextBytes(DATA);
        new File("target").mkdir();
        testAsyncIO(setup("target/test1.txt"));
        testSyncIO(setup("target/test2.txt"));
        testBIO(setup("target/test3.txt"));
        testMmap(setup("target/test4.txt"));
    }

    private static File setup(String name) throws Exception {
        RandomAccessFile f = new RandomAccessFile(name, "rw");
        f.setLength((WARMUP_COUNT + COUNT) * SIZE);
        f.getFD().sync();
        f.close();
        return new File(name);
    }

    private static void testBIO(File file) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(DATA);
        FileOutputStream fos = new FileOutputStream(file);
        for (int i = 0; i < WARMUP_COUNT; i++) {
            buf.clear();
            fos.write(buf.array());
            fos.getFD().sync();
        }
        long total = 0;
        for (int i = 0; i < COUNT; i++) {
            buf.clear();
            fos.write(buf.array());
            fos.flush();
            long t = System.nanoTime();
            fos.getFD().sync();
            total += System.nanoTime() - t;
        }
        System.out.println("bio sync avg time: " + total / COUNT / 1000 + " us");
        fos.close();
        Files.delete(file.toPath());
    }

    private static void testSyncIO(File file) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(DATA);
        FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        long pos = 0;
        for (int i = 0; i < WARMUP_COUNT; i++) {
            buf.clear();
            int bytes = fc.write(buf, pos);
            if (bytes != SIZE) {
                // keep simple
                throw new RuntimeException("write error");
            }
            pos += SIZE;
            fc.force(false);
        }
        long total = 0;
        for (int i = 0; i < COUNT; i++) {
            buf.clear();
            int bytes = fc.write(buf, pos);
            if (bytes != SIZE) {
                // keep simple
                throw new RuntimeException("write error");
            }
            pos += SIZE;
            long t = System.nanoTime();
            fc.force(false);
            total += System.nanoTime() - t;
        }
        System.out.println("sync force avg time: " + total / COUNT / 1000 + " us");
        fc.close();
        Files.delete(file.toPath());
    }

    private static void testAsyncIO(File file) throws Exception {
        AsynchronousFileChannel c = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        long pos = 0;
        for (int i = 0; i < WARMUP_COUNT; i++) {
            ByteBuffer buf = ByteBuffer.wrap(DATA);
            int bytes = c.write(buf, pos).get();
            if (bytes != SIZE) {
                // keep simple
                throw new RuntimeException("write error");
            }
            pos += SIZE;
            c.force(false);
        }
        long total = 0;
        for (int i = 0; i < COUNT; i++) {
            ByteBuffer buf = ByteBuffer.wrap(DATA);
            int bytes = c.write(buf, pos).get();
            if (bytes != SIZE) {
                // keep simple
                throw new RuntimeException("write error");
            }
            pos += SIZE;
            long t = System.nanoTime();
            c.force(false);
            total += System.nanoTime() - t;
        }
        System.out.println("async force avg time: " + total / COUNT / 1000 + " us");
        c.close();
        Files.delete(file.toPath());
    }

    private static void testMmap(File file) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(DATA);
        FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        MappedByteBuffer mapBuffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, fc.size());
        for (int i = 0; i < WARMUP_COUNT; i++) {
            buf.clear();
            mapBuffer.put(buf);
            mapBuffer.force();
        }
        long total = 0;
        for (int i = 0; i < COUNT; i++) {
            buf.clear();
            mapBuffer.put(buf);
            long t = System.nanoTime();
            mapBuffer.force();
            total += System.nanoTime() - t;
        }
        System.out.println("mmap force avg time: " + total / COUNT / 1000 + " us");
        fc.close();
        Files.delete(file.toPath());
    }
}

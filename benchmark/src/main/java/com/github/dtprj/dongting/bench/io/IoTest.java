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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author huangli
 */
public class IoTest {
    protected static final int FILE_SIZE = 128 * 1024 * 1024;
    protected static final int BUFFER_SIZE = 4 * 1024;
    protected static final int MAX_PENDING = 1024;

    protected static final int COUNT = FILE_SIZE / BUFFER_SIZE;

    protected static final byte[] DATA = new byte[BUFFER_SIZE];

    private File createFile(String name) throws Exception {
        File dir = new File("target");
        if (!dir.exists() && !dir.mkdirs()) {
            throw new Exception("create dir failed");
        }
        File f = new File(dir, name);
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        raf.setLength(FILE_SIZE);
        raf.getFD().sync();
        raf.close();
        return f;
    }

    public static void main(String[] args) throws Exception {
        new Random().nextBytes(DATA);
        new IoTest().start();
    }

    private void start() throws Exception {
        test1();
        System.out.println("-------------------");
        test2();
    }

    private void test1() throws Exception {
        File file = createFile("test1");
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(file.toPath(),
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        long startTime = System.nanoTime();
        LongAdder totalWriteLatencyNanos = new LongAdder();
        long totalSyncNanos = 0;
        long totalLatencyNanos = 0;
        LinkedList<Long> queue = new LinkedList<>();
        int totalSyncCount = 0;
        for (int i = 0; i < COUNT; ) {
            CountDownLatch latch = new CountDownLatch(MAX_PENDING);
            for (int j = 0; j < MAX_PENDING; j++, i++) {
                long itemStart = System.nanoTime();
                queue.add(itemStart);
                channel.write(ByteBuffer.wrap(DATA), (long) i * BUFFER_SIZE,
                        null, new CompletionHandler<Integer, Void>() {
                            @Override
                            public void completed(Integer result, Void attachment) {
                                if (result != BUFFER_SIZE) {
                                    System.out.println("result=" + result);
                                    System.exit(1);
                                }
                                totalWriteLatencyNanos.add(System.nanoTime() - itemStart);
                                latch.countDown();
                            }

                            @Override
                            public void failed(Throwable exc, Void attachment) {
                                exc.printStackTrace();
                            }
                        });
            }
            latch.await();
            long t = System.nanoTime();
            channel.force(false);
            long now = System.nanoTime();
            totalSyncNanos += now - t;
            totalSyncCount++;
            for (Long itemStart : queue) {
                totalLatencyNanos += now - itemStart;
            }
            queue.clear();
        }
        long totalTime = System.nanoTime() - startTime;
        System.out.println("avg sync latency: " + totalLatencyNanos / COUNT / 1000 + " us");
        System.out.println("avg write latency: " + totalWriteLatencyNanos.sum() / COUNT / 1000 + " us");
        System.out.println("tps: " + 1000L * 1000 * 1000 * COUNT / totalTime);

        System.out.println("avg sync time: " + totalSyncNanos / totalSyncCount / 1000 + " us");

        System.out.println("total time: " + totalTime / 1000 / 1000 + " ms");

        channel.close();
        Files.delete(file.toPath());
    }

    private void test2() throws Exception {
        int FILE_COUNT = 2;
        File[] files = new File[FILE_COUNT];
        AsynchronousFileChannel[] channels = new AsynchronousFileChannel[FILE_COUNT];
        ArrayList<LinkedList<Long>> queues = new ArrayList<>();
        for (int i = 0; i < FILE_COUNT; i++) {
            files[i] = createFile("test" + i);
            channels[i] = AsynchronousFileChannel.open(files[i].toPath(),
                    StandardOpenOption.WRITE, StandardOpenOption.READ);
            queues.add(new LinkedList<>());
        }
        long startTime = System.nanoTime();
        LongAdder totalWriteLatencyNanos = new LongAdder();
        long totalSyncNanos = 0;
        long totalLatencyNanos = 0;

        int totalSyncCount = 0;
        for (int i = 0; i < COUNT; ) {
            CountDownLatch latch = new CountDownLatch(MAX_PENDING);
            for (int j = 0; j < MAX_PENDING; j++, i++) {
                int idx = j % FILE_COUNT;
                long itemStart = System.nanoTime();
                queues.get(idx).add(itemStart);
                channels[idx].write(ByteBuffer.wrap(DATA), (long) i * BUFFER_SIZE,
                        null, new CompletionHandler<Integer, Void>() {
                            @Override
                            public void completed(Integer result, Void attachment) {
                                if (result != BUFFER_SIZE) {
                                    System.out.println("result=" + result);
                                    System.exit(1);
                                }
                                totalWriteLatencyNanos.add(System.nanoTime() - itemStart);
                                latch.countDown();
                            }

                            @Override
                            public void failed(Throwable exc, Void attachment) {
                                exc.printStackTrace();
                            }
                        });
            }
            latch.await();

            for (int idx = 0; idx < FILE_COUNT; idx++) {
                long t = System.nanoTime();
                channels[idx].force(false);
                long now = System.nanoTime();
                totalSyncNanos += now - t;
                totalSyncCount++;
                LinkedList<Long> queue = queues.get(idx);
                for (Long itemStart : queue) {
                    totalLatencyNanos += now - itemStart;
                }
                queue.clear();
            }
        }
        long totalTime = System.nanoTime() - startTime;
        System.out.println("avg sync latency: " + totalLatencyNanos / COUNT / 1000 + " us");
        System.out.println("avg write latency: " + totalWriteLatencyNanos.sum() / COUNT / 1000 + " us");
        System.out.println("tps: " + 1000L * 1000 * 1000 * COUNT / totalTime);

        System.out.println("avg sync time: " + totalSyncNanos / totalSyncCount / 1000 + " us");

        System.out.println("total time: " + totalTime / 1000 / 1000 + " ms");


        for (int i = 0; i < FILE_COUNT; i++) {
            channels[i].close();
            Files.delete(files[i].toPath());
        }
    }
}

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
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * write instantly, sync in batch.
 *
 * @author huangli
 */
@SuppressWarnings({"CallToPrintStackTrace", "SizeReplaceableByIsEmpty"})
public class IoMode1 implements CompletionHandler<Integer, IoMode1.WriteTask> {

    private static final int FILE_SIZE = 50 * 1024 * 1024;
    private static final int BUFFER_SIZE = 4 * 1024;
    private static final int MAX_PENDING = 1000;

    private static final int COUNT = FILE_SIZE / BUFFER_SIZE;

    private static final byte[] DATA = new byte[BUFFER_SIZE];

    private final LinkedList<WriteTask> waitWriteFinishQueue = new LinkedList<>();
    private final LinkedList<WriteTask> waitSyncFinishQueue = new LinkedList<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition syncFinish = lock.newCondition();
    private final Condition writeFinish = lock.newCondition();
    private int writeFinishIndex;
    private int syncFinishIndex;
    private long startTime;

    private final File file = new File("target/test");
    private AsynchronousFileChannel channel;

    private long totalWriteLatencyNanos;

    public static void main(String[] args) throws Exception {
        new IoMode1().start();
    }

    public static class WriteTask {
        int index;
        long start;
        boolean finish;
    }

    public void start() throws Exception {
        new Random().nextBytes(DATA);
        new File("target").mkdir();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(FILE_SIZE);
        raf.getFD().sync();
        raf.close();

        channel = AsynchronousFileChannel.open(file.toPath(),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.READ);
        startTime = System.nanoTime();
        new Thread(() -> {
            try {
                startWriter();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(() -> {
            try {
                startSync();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

    }

    private void startWriter() throws Exception {
        long pos = 0;
        for (int writeBeginIndex = 0; writeBeginIndex < COUNT; writeBeginIndex++) {
            ByteBuffer buf = ByteBuffer.wrap(DATA);
            WriteTask task = new WriteTask();
            lock.lock();
            try {
                while (writeBeginIndex - syncFinishIndex >= MAX_PENDING) {
                    syncFinish.await();
                }
                task.start = System.nanoTime();
                task.index = writeBeginIndex;
                waitWriteFinishQueue.add(task);
            } finally {
                lock.unlock();
            }
            channel.write(buf, pos, task, this);
            pos += BUFFER_SIZE;
        }
    }

    @Override
    public void completed(Integer result, WriteTask task) {
        if (result != BUFFER_SIZE) {
            // keep simple
            System.out.println("write not complete");
            System.exit(1);
        }
        task.finish = true;
        totalWriteLatencyNanos += System.nanoTime() - task.start;
        lock.lock();

        try {
            boolean notify = false;
            while (waitWriteFinishQueue.size() > 0) {
                WriteTask t = waitWriteFinishQueue.getFirst();
                if (t.finish) {
                    waitWriteFinishQueue.removeFirst();
                    writeFinishIndex++;
                    waitSyncFinishQueue.add(t);
                    notify = true;
                } else {
                    break;
                }
            }
            if (notify) {
                writeFinish.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void failed(Throwable exc, WriteTask attachment) {
        exc.printStackTrace();
        System.exit(1);
    }

    private void startSync() throws Exception {
        long totalSyncNanos = 0;
        int totalTimes = 0;
        long totalLatencyNanos = 0;
        for (int flushBeginIndex = 0; flushBeginIndex < COUNT; ) {
            lock.lock();
            try {
                while (syncFinishIndex - writeFinishIndex == 0) {
                    writeFinish.await();
                }
                flushBeginIndex = writeFinishIndex;
            } finally {
                lock.unlock();
            }

            long startNanos = System.nanoTime();
            channel.force(false);
            long endNanos = System.nanoTime();
            totalSyncNanos += endNanos - startNanos;
            totalTimes++;

            lock.lock();
            try {
                syncFinishIndex = flushBeginIndex;
                syncFinish.signal();
                while (waitSyncFinishQueue.size() > 0) {
                    WriteTask t = waitSyncFinishQueue.getFirst();
                    if (t.index <= flushBeginIndex) {
                        waitSyncFinishQueue.removeFirst();
                        totalLatencyNanos += endNanos - t.start;
                    } else {
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        channel.close();

        System.out.println("avg sync time: " + totalSyncNanos / totalTimes / 1000 + " us");
        System.out.println("avg sync batch: " + 1.0 * COUNT / totalTimes);
        System.out.println("avg write latency: " + totalWriteLatencyNanos / COUNT / 1000 + " us");
        System.out.println("avg sync latency: " + totalLatencyNanos / COUNT / 1000 + " us");

        long totalTime = System.nanoTime() - startTime;
        System.out.println("total time: " + totalTime / 1000 / 1000 + " ms");
        System.out.println("tps: " + 1000L * 1000 * 1000 * COUNT / totalTime);
        Files.delete(file.toPath());
    }
}

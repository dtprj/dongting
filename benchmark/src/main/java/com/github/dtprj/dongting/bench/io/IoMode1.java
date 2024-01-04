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
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * write instantly, sync in batch.
 *
 * @author huangli
 */
@SuppressWarnings({"CallToPrintStackTrace", "SizeReplaceableByIsEmpty"})
public class IoMode1 extends IoModeBase implements CompletionHandler<Integer, WriteTask> {

    private final LinkedList<WriteTask> waitWriteFinishQueue = new LinkedList<>();
    private final LinkedList<WriteTask> waitSyncFinishQueue = new LinkedList<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition syncFinish = lock.newCondition();
    private final Condition writeFinish = lock.newCondition();
    private int writeFinishIndex = -1;
    private int syncFinishIndex = -1;

    private final File file;
    private final AsynchronousFileChannel channel;

    private long totalWriteLatencyNanos;

    public static void main(String[] args) throws Exception {
        new IoMode1().start();
    }

    public IoMode1() throws Exception {
        file = createFile("testIO1");
        channel = AsynchronousFileChannel.open(file.toPath(),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.READ);
    }

    @Override
    protected void startWriter() throws Exception {
        long pos = 0;
        for (int writeBeginIndex = 0; writeBeginIndex < COUNT; writeBeginIndex++) {
            ByteBuffer buf = ByteBuffer.wrap(DATA);
            WriteTask task = new WriteTask();
            task.writeBeginNanos = System.nanoTime();
            task.index = writeBeginIndex;
            lock.lock();
            try {
                while (writeBeginIndex - syncFinishIndex >= MAX_PENDING) {
                    syncFinish.await();
                }
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
        task.writeFinish = true;
        totalWriteLatencyNanos += System.nanoTime() - task.writeBeginNanos;
        lock.lock();

        try {
            boolean notify = false;
            while (waitWriteFinishQueue.size() > 0) {
                WriteTask t = waitWriteFinishQueue.getFirst();
                if (t.writeFinish) {
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

    @Override
    protected void startSync() throws Exception {
        long totalSyncNanos = 0;
        int totalTimes = 0;
        long totalLatencyNanos = 0;
        while (syncFinishIndex < COUNT - 1) {
            lock.lock();
            int syncBeginIndex;
            try {
                while (waitSyncFinishQueue.size() == 0) {
                    writeFinish.await();
                }
                syncBeginIndex = writeFinishIndex;
            } finally {
                lock.unlock();
            }

            long startNanos = System.nanoTime();
            channel.force(false);
            totalSyncNanos += System.nanoTime() - startNanos;
            totalTimes++;

            lock.lock();
            try {
                long now = System.nanoTime();
                syncFinishIndex = syncBeginIndex;
                syncFinish.signal();
                while (waitSyncFinishQueue.size() > 0) {
                    WriteTask t = waitSyncFinishQueue.getFirst();
                    if (t.index <= syncBeginIndex) {
                        waitSyncFinishQueue.removeFirst();
                        totalLatencyNanos += now - t.writeBeginNanos;
                    } else {
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        long totalTime = System.nanoTime() - startTime;
        System.out.println("avg sync latency: " + totalLatencyNanos / COUNT / 1000 + " us");
        System.out.println("avg write latency: " + totalWriteLatencyNanos / COUNT / 1000 + " us");
        System.out.println("tps: " + 1000L * 1000 * 1000 * COUNT / totalTime);

        System.out.println("avg sync time: " + totalSyncNanos / totalTimes / 1000 + " us");
        System.out.println("avg sync batch: " + 1.0 * COUNT / totalTimes);

        System.out.println("total time: " + totalTime / 1000 / 1000 + " ms");

        channel.close();
        Files.delete(file.toPath());
    }
}

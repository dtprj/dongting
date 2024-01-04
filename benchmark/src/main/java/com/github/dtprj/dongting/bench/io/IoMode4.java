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
 * don't write and sync concurrently.
 *
 * @author huangli
 */
@SuppressWarnings({"CallToPrintStackTrace", "SizeReplaceableByIsEmpty"})
public class IoMode4 extends IoModeBase implements CompletionHandler<Integer, IoMode4.Mode4WriteTask> {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition syncFinish = lock.newCondition();
    private final Condition writeFinish = lock.newCondition();
    private int syncFinishIndex = -1;

    private FileInfo writeFileInfo;
    private FileInfo syncFileInfo;

    private boolean sync;

    private long totalWriteLatencyNanos;

    public static class FileInfo {
        long pos;
        File file;
        AsynchronousFileChannel channel;
        final LinkedList<WriteTask> waitWriteFinishQueue = new LinkedList<>();
        final LinkedList<WriteTask> waitSyncFinishQueue = new LinkedList<>();
    }

    public static class Mode4WriteTask extends WriteTask {
        FileInfo fileInfo;
    }

    public static void main(String[] args) throws Exception {
        new IoMode4().start();
    }

    public IoMode4() throws Exception {
        FileInfo[] files = new FileInfo[2];
        for (int i = 0; i < 2; i++) {
            files[i] = new FileInfo();
            files[i].file = createFile("test" + i);
            files[i].channel = AsynchronousFileChannel.open(files[i].file.toPath(),
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.READ);
        }
        writeFileInfo = files[0];
        syncFileInfo = files[1];
    }

    @Override
    protected void startWriter() throws Exception {
        for (int writeBeginIndex = 0; writeBeginIndex < COUNT; writeBeginIndex++) {
            ByteBuffer buf = ByteBuffer.wrap(DATA);
            Mode4WriteTask task = new Mode4WriteTask();
            task.writeBeginNanos = System.nanoTime();
            task.index = writeBeginIndex;
            lock.lock();
            FileInfo fi;
            try {
                while (writeBeginIndex - syncFinishIndex >= MAX_PENDING) {
                    syncFinish.await();
                }
                if (!sync && writeFileInfo.waitSyncFinishQueue.size() > 0 || writeFileInfo.waitWriteFinishQueue.size() > 0) {
                    swap();
                }
                task.fileInfo = writeFileInfo;
                writeFileInfo.waitWriteFinishQueue.add(task);
            } finally {
                fi = writeFileInfo;
                lock.unlock();
            }
            fi.channel.write(buf, fi.pos, task, this);
            fi.pos += BUFFER_SIZE;
        }
        lock.lock();
        try {
            if (!sync) {
                swap();
            } else {
                syncFinish.await();
                swap();
            }
        } finally {
            lock.unlock();
        }
    }

    private void swap() {
        FileInfo tmp = writeFileInfo;
        writeFileInfo = syncFileInfo;
        syncFileInfo = tmp;
        if (syncFileInfo.waitWriteFinishQueue.size() == 0) {
            writeFinish.signal();
        }
    }

    @Override
    public void completed(Integer result, IoMode4.Mode4WriteTask task) {
        if (result != BUFFER_SIZE) {
            // keep simple
            System.out.println("write not complete");
            System.exit(1);
        }
        task.writeFinish = true;
        totalWriteLatencyNanos += System.nanoTime() - task.writeBeginNanos;
        FileInfo fi = task.fileInfo;
        lock.lock();
        try {
            while (fi.waitWriteFinishQueue.size() > 0) {
                WriteTask t = fi.waitWriteFinishQueue.getFirst();
                if (t.writeFinish) {
                    fi.waitWriteFinishQueue.removeFirst();
                    fi.waitSyncFinishQueue.add(t);
                } else {
                    break;
                }
            }
            if (fi == syncFileInfo && fi.waitWriteFinishQueue.size() == 0) {
                writeFinish.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void failed(Throwable exc, IoMode4.Mode4WriteTask attachment) {
        exc.printStackTrace();
        System.exit(1);
    }

    @Override
    protected void startSync() throws Exception {
        long totalSyncNanos = 0;
        int totalTimes = 0;
        long totalLatencyNanos = 0;
        while (syncFinishIndex < COUNT - 1) {
            int syncBeginIndex;
            lock.lock();
            try {
                while (syncFileInfo.waitWriteFinishQueue.size() > 0 || syncFileInfo.waitSyncFinishQueue.size() == 0) {
                    writeFinish.await();
                }
                syncBeginIndex = syncFileInfo.waitSyncFinishQueue.getLast().index;
                sync = true;
            } finally {
                lock.unlock();
            }

            long startNanos = System.nanoTime();
            syncFileInfo.channel.force(false);
            totalSyncNanos += System.nanoTime() - startNanos;
            totalTimes++;

            lock.lock();
            try {
                long now = System.nanoTime();
                syncFinishIndex = syncBeginIndex;
                syncFinish.signal();
                while (syncFileInfo.waitSyncFinishQueue.size() > 0) {
                    WriteTask t = syncFileInfo.waitSyncFinishQueue.removeFirst();
                    totalLatencyNanos += now - t.writeBeginNanos;
                }
            } finally {
                sync = false;
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


        writeFileInfo.channel.close();
        Files.delete(writeFileInfo.file.toPath());
        syncFileInfo.channel.close();
        Files.delete(syncFileInfo.file.toPath());
    }
}

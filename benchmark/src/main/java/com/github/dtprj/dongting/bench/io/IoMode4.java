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

    private static final int FILE_COUNT = 4;
    private static final int MAX_BATCH = MAX_PENDING / (FILE_COUNT - 1);

    private final FileInfo[] files = new FileInfo[FILE_COUNT];

    private int writeFileIndex;
    private int syncFileIndex;

    private long totalWriteLatencyNanos;

    public static class FileInfo {
        long pos;
        File file;
        AsynchronousFileChannel channel;
        final LinkedList<WriteTask> waitWriteFinishQueue = new LinkedList<>();
        final LinkedList<WriteTask> waitSyncFinishQueue = new LinkedList<>();

        boolean prepareSync;
        boolean sync;
    }

    public static class Mode4WriteTask extends WriteTask {
        FileInfo fileInfo;
    }

    public static void main(String[] args) throws Exception {
        new IoMode4().start();
    }

    public IoMode4() throws Exception {
        for (int i = 0; i < files.length; i++) {
            files[i] = new FileInfo();
            files[i].file = createFile("test" + i);
            files[i].channel = AsynchronousFileChannel.open(files[i].file.toPath(),
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.READ);
        }
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
                while (true) {
                    if (syncFileIndex > writeFileIndex) {
                        writeFileIndex = syncFileIndex;
                    }
                    fi = files[writeFileIndex % files.length];
                    if (syncFileIndex == writeFileIndex) {
                        if (fi.prepareSync && (fi.waitWriteFinishQueue.size() > 0 || fi.waitSyncFinishQueue.size() > 0)) {
                            writeFileIndex++;
                            continue;
                        }
                    }
                    if (fi.sync) {
                        syncFinish.await();
                        break;
                    } else if (fi.waitWriteFinishQueue.size() + fi.waitSyncFinishQueue.size() >= MAX_BATCH) {
                        writeFileIndex++;
                    } else if (writeBeginIndex - syncFinishIndex >= MAX_PENDING) {
                        syncFinish.await();
                    } else {
                        break;
                    }
                }

                task.fileInfo = fi;
                fi.waitWriteFinishQueue.add(task);
            } finally {
                lock.unlock();
            }
            fi.channel.write(buf, fi.pos, task, this);
            fi.pos += BUFFER_SIZE;
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
            if (fi.waitWriteFinishQueue.size() == 0) {
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
            FileInfo fi;
            lock.lock();
            try {
                fi = files[syncFileIndex % files.length];
                fi.prepareSync = true;
                while (fi.waitWriteFinishQueue.size() > 0 || fi.waitSyncFinishQueue.size() == 0) {
                    writeFinish.await();
                }
                fi.sync = true;
                syncBeginIndex = fi.waitSyncFinishQueue.getLast().index;
            } finally {
                lock.unlock();
            }

            long startNanos = System.nanoTime();
            fi.channel.force(false);
            totalSyncNanos += System.nanoTime() - startNanos;
            totalTimes++;

            lock.lock();
            try {
                long now = System.nanoTime();
                syncFinishIndex = syncBeginIndex;
                syncFinish.signal();
                while (fi.waitSyncFinishQueue.size() > 0) {
                    WriteTask t = fi.waitSyncFinishQueue.removeFirst();
                    totalLatencyNanos += now - t.writeBeginNanos;
                }
                syncFileIndex++;
                fi.prepareSync = false;
                fi.sync = false;
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


        for (FileInfo fi : files) {
            fi.channel.close();
            Files.delete(fi.file.toPath());
        }
    }
}

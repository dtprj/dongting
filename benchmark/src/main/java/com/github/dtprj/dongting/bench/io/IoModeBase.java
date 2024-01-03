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
import java.util.Random;

/**
 * @author huangli
 */
public abstract class IoModeBase {
    protected static final int FILE_SIZE = 300 * 1024 * 1024;
    protected static final int BUFFER_SIZE = 4 * 1024;
    protected static final int MAX_PENDING = 1000;

    protected static final int COUNT = FILE_SIZE / BUFFER_SIZE;

    protected static final byte[] DATA = new byte[BUFFER_SIZE];

    protected long startTime;

    protected final File file = new File("target/test");

    public IoModeBase() throws Exception {
        new Random().nextBytes(DATA);
        new File("target").mkdir();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(FILE_SIZE);
        raf.getFD().sync();
        raf.close();
    }

    public void start() throws Exception {
        startTime = System.nanoTime();
        new Thread(() -> {
            try {
                startWriter();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "writer").start();
        new Thread(() -> {
            try {
                startSync();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "syncer").start();
    }

    protected abstract void startWriter() throws Exception;
    protected abstract void startSync() throws Exception;

}

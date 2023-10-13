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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.common.RunnableEx;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.UnrecoverableException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * @author huangli
 */
public class FileUtil {
    private static final DtLog log = DtLogs.getLogger(FileUtil.class);

    public static File ensureDir(String dataDir) {
        File dir = new File(dataDir);
        return ensureDir(dir);
    }

    public static File ensureDir(File parentDir, String subDirName) {
        File dir = new File(parentDir, subDirName);
        return ensureDir(dir);
    }

    public static File ensureDir(File dir) {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RaftException("make dir failed: " + dir.getPath());
            }
            log.info("make dir: {}", dir.getPath());
        }
        return dir;
    }

    public static void syncReadFull(AsynchronousFileChannel c, ByteBuffer buf, long pos) throws IOException {
        while (buf.hasRemaining()) {
            Future<Integer> f = c.read(buf, pos);
            pos += getResult(f);
        }
    }

    public static int syncRead(AsynchronousFileChannel c, ByteBuffer buf, long pos) throws IOException {
        Future<Integer> f = c.read(buf, pos);
        return getResult(f);
    }

    private static int getResult(Future<Integer> f) throws IOException {
        try {
            return f.get();
        } catch (InterruptedException e) {
            throw new RaftException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new RaftException(e.getCause());
            }
        }
    }

    public static void doWithRetry(RunnableEx<Exception> callback, Supplier<Boolean> stopIndicator, boolean closing,
                                   int... retryIntervals) throws InterruptedException {
        int retry = 0;
        while (true) {
            if (!closing && stopIndicator.get()) {
                throw new StoppedException();
            }
            try {
                callback.run();
                return;
            } catch (Exception e) {
                Throwable root = DtUtil.rootCause(e);
                if (root instanceof StoppedException) {
                    throw (StoppedException) root;
                }
                if (root instanceof UnrecoverableException) {
                    throw (UnrecoverableException) root;
                }
                if (root instanceof InterruptedException) {
                    throw (InterruptedException) root;
                }
                if (retryIntervals != null && retry < retryIntervals.length) {
                    int sleep = retryIntervals[retry];
                    log.error("io error occurs, retry after {} ms", sleep, e);
                    //noinspection BusyWait
                    Thread.sleep(sleep);
                    retry++;
                } else {
                    log.error("io error occurs", e);
                    throw new UnrecoverableException("io error occurs", e);
                }
            }
        }
    }

    public static String baseName(File f) {
        String name = f.getName();
        int i = name.lastIndexOf('.');
        if (i > 0) {
            return name.substring(0, i);
        } else {
            return name;
        }
    }
}

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

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

    public static long syncWriteFull(AsynchronousFileChannel c, ByteBuffer buf, long pos) throws IOException {
        while (buf.hasRemaining()) {
            Future<Integer> f = c.write(buf, pos);
            pos += getResult(f);
        }
        return pos;
    }
}

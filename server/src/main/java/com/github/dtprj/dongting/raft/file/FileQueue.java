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
package com.github.dtprj.dongting.raft.file;

import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.client.RaftException;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author huangli
 */
abstract class FileQueue {
    private static final DtLog log = DtLogs.getLogger(FileQueue.class);
    private static final Pattern PATTERN = Pattern.compile("^(\\d{20})$");
    protected final IndexedQueue<LogFile> queue = new IndexedQueue<>();
    protected final Executor ioExecutor;

    protected long queueStartPosition;
    protected long queueEndPosition;

    public FileQueue(Executor ioExecutor) {
        this.ioExecutor = ioExecutor;
    }

    protected abstract long getFileSize();

    public void init(File dir) throws IOException {
        File[] files = dir.listFiles();
        Arrays.sort(files);
        for (File f : files) {
            if (!f.isFile()) {
                continue;
            }
            Matcher matcher = PATTERN.matcher(f.getName());
            if (matcher.matches()) {
                if (f.length() > getFileSize()) {
                    log.error("file size error: {}, size={}", f.getPath(), f.length());
                    throw new RaftException("file size error");
                }
                long startIndex = Long.parseLong(matcher.group(1));
                log.info("load file: {}", f.getPath());
                FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
                queue.addLast(new LogFile(startIndex, startIndex + getFileSize(), channel));
                if (startIndex != 0) {
                    queueStartPosition = Math.min(queueStartPosition, startIndex);
                }
                queueEndPosition = Math.max(queueEndPosition, startIndex + getFileSize() - 1);
            }
        }
        for (int i = 0; i < queue.size(); i++) {
            LogFile lf = queue.get(i);
            if (lf.startIndex % getFileSize() != 0) {
                throw new RaftException("file start index error: " + lf.startIndex);
            }
            if (i != 0 && lf.startIndex != queue.get(i - 1).endIndex) {
                throw new RaftException("not follow previous file " + lf.startIndex);
            }
        }
    }

    protected LogFile getLogFile(long filePos) {
        long fileStartPos = filePos / getFileSize();
        int index = (int) ((fileStartPos - queueStartPosition) / getFileSize());
        return queue.get(index);
    }

    public void close() {
        for (int i = 0; i < queue.size(); i++) {
            try {
                queue.get(i).channel.close();
            } catch (IOException e) {
                log.error("close file error", e);
            }
        }
    }
}

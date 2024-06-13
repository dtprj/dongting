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

import com.github.dtprj.dongting.common.DtUtil;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.log.DtLog;
import com.github.dtprj.dongting.log.DtLogs;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.impl.RaftUtil;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.server.RaftGroupConfigEx;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.zip.CRC32C;

/**
 * @author huangli
 */
public class StatusFile implements AutoCloseable {
    private static final DtLog log = DtLogs.getLogger(StatusFile.class);

    // we think the minimum storage write unit is 4k
    private static final int FILE_LENGTH = 4096;
    private static final int CRC_HEX_LENGTH = 8;

    private final File file;
    private final FiberGroup fiberGroup;
    private final RaftGroupConfigEx groupConfig;
    private FileLock lock;
    private DtFile dtFile;
    private final CRC32C crc32c = new CRC32C();

    private final Map<String, String> properties = new HashMap<>();

    public StatusFile(File file, RaftGroupConfigEx groupConfig) {
        this.file = file;
        this.fiberGroup = groupConfig.getFiberGroup();
        this.groupConfig = groupConfig;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public FiberFrame<Void> init() {
        return new FiberFrame<>() {
            private final ByteBuffer buf = groupConfig.getHeapPool().getPool().allocate(FILE_LENGTH);

            @Override
            protected FrameCallResult doFinally() {
                if (buf != null) {
                    groupConfig.getHeapPool().getPool().release(buf);
                }
                return Fiber.frameReturn();
            }

            @Override
            public FrameCallResult execute(Void input) throws Exception {
                boolean needLoad = file.exists() && file.length() != 0;
                HashSet<OpenOption> options = new HashSet<>();
                options.add(StandardOpenOption.CREATE);
                options.add(StandardOpenOption.READ);
                options.add(StandardOpenOption.WRITE);
                AsynchronousFileChannel channel = AsynchronousFileChannel.open(file.toPath(), options,
                        getFiberGroup().getExecutor());
                dtFile = new DtFile(file, channel, fiberGroup);
                lock = channel.tryLock();
                if (!needLoad) {
                    return Fiber.frameReturn();
                }
                if (file.length() != FILE_LENGTH) {
                    throw new RaftException("bad status file length: " + file.length());
                }
                AsyncIoTask task = new AsyncIoTask(groupConfig, dtFile);
                FiberFuture<Void> f = task.read(buf, 0);
                return f.await(this::resumeAfterRead);
            }

            private FrameCallResult resumeAfterRead(Void input) {
                crc32c.reset();
                byte[] arr = buf.array();
                crc32c.update(arr, CRC_HEX_LENGTH, FILE_LENGTH - CRC_HEX_LENGTH);
                int expectCrc = (int) crc32c.getValue();
                int actualCrc = Integer.parseUnsignedInt(new String(arr, 0, CRC_HEX_LENGTH,
                        StandardCharsets.UTF_8), 16);

                if (actualCrc != expectCrc) {
                    throw new ChecksumException("bad status file crc: " + actualCrc + ", expect: " + expectCrc);
                }

                String key = null;
                for (int i = CRC_HEX_LENGTH + 1, s = i; i < FILE_LENGTH; i++) {
                    if (key == null) {
                        if (arr[i] == '=') {
                            key = new String(arr, s, i - s, StandardCharsets.UTF_8);
                            s = i + 1;
                        }
                    } else {
                        if (arr[i] == '\n') {
                            String value = new String(arr, s, i - s, StandardCharsets.UTF_8);
                            properties.put(key, value);
                            key = null;
                            s = i + 1;
                        }
                    }
                }
                log.info("loaded status file: {}, content: {}", file.getPath(), properties);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult handle(Throwable ex) {
                DtUtil.close(lock, dtFile.getChannel());
                throw new RaftException(ex);
            }
        };
    }

    public FiberFuture<Void> update(boolean sync) {
        try {
            ByteBuffer buf = groupConfig.getDirectPool().borrow(FILE_LENGTH);
            buf.position(CRC_HEX_LENGTH);
            buf.put((byte) '\n');
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] value = entry.getValue().getBytes(StandardCharsets.UTF_8);
                buf.put(key);
                buf.put((byte) '=');
                buf.put(value);
                buf.put((byte) '\n');
            }
            while (buf.hasRemaining()) {
                buf.put((byte) ' ');
            }

            crc32c.reset();
            RaftUtil.updateCrc(crc32c, buf, CRC_HEX_LENGTH, FILE_LENGTH - CRC_HEX_LENGTH);
            int crc = (int) crc32c.getValue();
            buf.clear();
            String crcHex = Integer.toHexString(crc);
            for (int i = 0, len = crcHex.length(); i < CRC_HEX_LENGTH - len; i++) {
                buf.put((byte) '0');
            }
            for(int i=0, len = crcHex.length(); i < len; i++) {
                buf.put((byte) crcHex.charAt(i));
            }
            buf.clear();

            // retry in status manager
            AsyncIoTask task = new AsyncIoTask(groupConfig, dtFile);
            FiberFuture<Void> f;
            if (sync) {
                f = task.writeAndForce(buf, 0, false);
            } else {
                f = task.write(buf, 0);
            }
            f.registerCallback((v, ex) -> groupConfig.getDirectPool().release(buf));
            return f;
        } catch (Throwable e) {
            RaftException raftException = new RaftException("update status file failed. file=" + file.getPath(), e);
            return FiberFuture.failedFuture(fiberGroup, raftException);
        }
    }

    @Override
    public void close() {
        if (dtFile.getChannel().isOpen()) {
            DtUtil.close(lock, dtFile.getChannel());
        }
    }

}

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

import com.github.dtprj.dongting.common.Pair;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FiberFuture;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.fiber.HandlerFrame;
import com.github.dtprj.dongting.fiber.SimpleFrame;
import com.github.dtprj.dongting.raft.RaftException;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

final class LogReadFrame extends FiberFrame<Boolean> {
    private final LogFileQueue logFiles;
    private final long[] positions;
    private final int[] lengths;
    private final int[] offsets;
    private final int count;
    private final ByteBuffer dest;

    private FileRead[] reads;
    private int readerCount;
    private int nextRead;
    private FiberFuture<Void> pending;

    LogReadFrame(LogFileQueue logFiles, long[] positions, int[] lengths, int[] offsets, int count, ByteBuffer dest) {
        this.logFiles = logFiles;
        this.positions = positions;
        this.lengths = lengths;
        this.offsets = offsets;
        this.count = count;
        this.dest = dest;
    }

    @Override
    public FrameCallResult execute(Void input) {
        if (getFiber().isDaemon()) {
            throw new RaftException("log read should not run in daemon fiber");
        }
        if (count < 0 || count > positions.length || count > lengths.length || count > offsets.length) {
            throw new IllegalArgumentException("invalid read count: " + count);
        }
        if (dest.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        for (int i = 0; i < count; i++) {
            int len = lengths[i];
            int offset = offsets[i];
            if (len <= 0 || offset < 0 || offset > dest.limit() || len > dest.limit() - offset) {
                throw new IllegalArgumentException("invalid read range at " + i);
            }
        }
        nextRead = 0;
        if (!prepareReads()) {
            return justReturn(false);
        }
        // Pin all files before the first suspension or I/O submission.
        for (FileRead read : reads) {
            readerCount++;
            read.file.incReaders();
        }
        return readNext(null);
    }

    private boolean prepareReads() {
        LinkedHashMap<MainLogFile, FileRead> byFile = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            long pos = positions[i];
            int len = lengths[i];
            if (pos < logFiles.raftStatus.firstValidPos || pos < logFiles.queueStartPosition) {
                return false;
            }
            MainLogFile file = logFiles.getLogFile(pos);
            if (file == null || file.deleted) {
                return false;
            }
            if (len > file.endPos - pos) {
                throw new IllegalArgumentException("record crosses file boundary: " + pos + ", " + len);
            }
            FileRead read = byFile.get(file);
            if (read == null) {
                read = new FileRead(file);
                byFile.put(file, read);
            }
            read.add(pos - file.startPos, offsets[i], len);
        }
        reads = byFile.values().toArray(new FileRead[0]);
        return true;
    }

    private FrameCallResult readNext(Void unused) {
        pending = null;
        if (nextRead == reads.length) {
            return justReturn(true);
        }
        FileRead read = reads[nextRead++];
        int size = read.ranges.size();
        AsyncIoTask task = new AsyncIoTask(getFiberGroup(), read.file);
        if (size == 1) {
            ReadRange range = read.ranges.get(0);
            pending = task.read(slice(range), range.position);
        } else {
            ByteBuffer[] buffers = new ByteBuffer[size];
            long[] filePositions = new long[size];
            for (int i = 0; i < size; i++) {
                ReadRange range = read.ranges.get(i);
                buffers[i] = slice(range);
                filePositions[i] = range.position;
            }
            pending = task.read(buffers, filePositions);
        }
        return pending.await(this::readNext);
    }

    private ByteBuffer slice(ReadRange range) {
        ByteBuffer view = dest.duplicate();
        view.limit(range.offset + range.length).position(range.offset);
        return view.slice();
    }

    @Override
    protected FrameCallResult doFinally() {
        return awaitPending(null);
    }

    private FrameCallResult awaitPending(Pair<Void, Throwable> unused) {
        if (pending != null && !pending.isDone()) {
            // An interrupted await must not release readers or return the caller's buffer early.
            return Fiber.call(new HandlerFrame<>(new SimpleFrame<Void>("waitLogRead",
                    f -> pending.await(f::justReturn))), this::awaitPending);
        }
        for (int i = 0; i < readerCount; i++) {
            reads[i].file.decReaders();
        }
        readerCount = 0;
        return Fiber.frameReturn();
    }

    private static final class FileRead {
        final MainLogFile file;
        final ArrayList<ReadRange> ranges = new ArrayList<>();

        FileRead(MainLogFile file) {
            this.file = file;
        }

        void add(long position, int offset, int length) {
            if (!ranges.isEmpty()) {
                ReadRange last = ranges.get(ranges.size() - 1);
                if (last.position + last.length == position && last.offset + last.length == offset) {
                    last.length += length;
                    return;
                }
            }
            ranges.add(new ReadRange(position, offset, length));
        }
    }

    private static final class ReadRange {
        final long position;
        final int offset;
        int length;

        ReadRange(long position, int offset, int length) {
            this.position = position;
            this.offset = offset;
            this.length = length;
        }
    }
}

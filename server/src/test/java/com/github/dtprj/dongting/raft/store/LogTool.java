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

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author huangli
 */
public class LogTool {
    private static final String DIR = "target/raft_data_1_node4/log";
    private static final long FILE_SIZE = 1024 * 1024 * 1024;
    private static final long MASK = FILE_SIZE - 1;

    private static LogHeader loadHeader(long pos) throws Exception {
        File dir = new File(DIR);
        String fileName = String.format("%020d", pos & ~MASK);
        int filePos = (int) (pos & MASK);
        try (FileChannel fc = FileChannel.open(new File(dir, fileName).toPath())) {
            MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_ONLY, 0, FILE_SIZE);
            buf.position(filePos);
            LogHeader header = new LogHeader();
            header.read(buf);
            return header;
        }
    }

    public static void main(String[] args) throws Exception {
        LogHeader h = loadHeader(0);
        System.out.println(h.crcMatch());
    }
}

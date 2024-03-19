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
import java.io.RandomAccessFile;

/**
 * @author huangli
 */
public class IdxTool {

    private static void getLogPos(long index) throws Exception {
        File dir = new File("target/raftlog/idx");
        long idxPos = index * 8;
        long mask = IdxFileQueue.DEFAULT_ITEMS_PER_FILE - 1;
        long posOfIdxFile = idxPos & mask;
        long fileStartPos = idxPos - posOfIdxFile;
        String fileName = String.format("%020d", fileStartPos);
        System.out.println("idx file=" + fileName + ",posOfIdxFile=" + posOfIdxFile);
        RandomAccessFile raf = new RandomAccessFile(new File(dir, fileName), "r");
        raf.seek(posOfIdxFile);
        long value = raf.readLong();
        System.out.println("idx=" + index + ", logPos=" + value);
        raf.close();
    }

    public static void main(String[] args) throws Exception {
        getLogPos(1411);
    }
}

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

import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;

import java.io.File;
import java.nio.channels.AsynchronousFileChannel;

/**
 * @author huangli
 */
class LogFile {
    final File file;
    final FiberCondition notUseCondition;
    final AsynchronousFileChannel channel;
    final long startPos;
    final long endPos;

    long firstTimestamp;
    long firstIndex;
    int firstTerm;

    private int use;
    long deleteTimestamp;
    boolean deleted;

    public LogFile(long startPos, long endPos, AsynchronousFileChannel channel,
                   File file, FiberCondition notUseCondition) {
        this.startPos = startPos;
        this.endPos = endPos;
        this.channel = channel;
        this.file = file;
        this.notUseCondition = notUseCondition;
    }

    public void incUseCount(){
        use++;
    }

    public void descUseCount() {
        use--;
        if (use == 0) {
            notUseCondition.signalAll();
        }
    }

    public FrameCallResult awaitNotUse(FrameCall<Void> resumePoint) throws Exception {
        if (use == 0) {
            return resumePoint.execute(null);
        } else {
            // loop to this method and recheck use count
            return notUseCondition.awaitOn(v -> awaitNotUse(resumePoint));
        }
    }

}

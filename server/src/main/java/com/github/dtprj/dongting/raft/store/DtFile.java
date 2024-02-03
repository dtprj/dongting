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

import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberCondition;
import com.github.dtprj.dongting.fiber.FiberGroup;
import com.github.dtprj.dongting.fiber.FrameCall;
import com.github.dtprj.dongting.fiber.FrameCallResult;

import java.io.File;
import java.nio.channels.AsynchronousFileChannel;

/**
 * @author huangli
 */
public class DtFile {
    private final File file;
    private final FiberCondition notUseCondition;
    private final AsynchronousFileChannel channel;

    private int use;

    DtFile(File file, AsynchronousFileChannel channel, FiberGroup fiberGroup) {
        this.file = file;
        this.channel = channel;
        this.notUseCondition = fiberGroup.newCondition("FileNotUse-" + file.getName());
    }

    public void incUseCount(){
        use++;
    }

    public void descUseCount() {
        use--;
        if (use == 0) {
            getNotUseCondition().signalAll();
        }
    }

    public FrameCallResult awaitNotUse(FrameCall<Void> resumePoint) {
        if (use == 0) {
            return Fiber.resume(null, resumePoint);
        } else {
            // loop to this method and recheck use count
            return getNotUseCondition().await(v -> awaitNotUse(resumePoint));
        }
    }

    public File getFile() {
        return file;
    }

    public FiberCondition getNotUseCondition() {
        return notUseCondition;
    }

    public AsynchronousFileChannel getChannel() {
        return channel;
    }
}

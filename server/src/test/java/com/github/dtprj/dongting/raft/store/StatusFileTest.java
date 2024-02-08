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
import com.github.dtprj.dongting.fiber.BaseFiberTest;
import com.github.dtprj.dongting.fiber.Fiber;
import com.github.dtprj.dongting.fiber.FiberFrame;
import com.github.dtprj.dongting.fiber.FrameCallResult;
import com.github.dtprj.dongting.raft.RaftException;
import com.github.dtprj.dongting.raft.server.ChecksumException;
import com.github.dtprj.dongting.raft.test.MockExecutors;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author huangli
 */
@SuppressWarnings("resource")
public class StatusFileTest extends BaseFiberTest {
    private static void update(File file, Properties data) throws Exception {
        StatusFile statusFile = new StatusFile(file, MockExecutors.ioExecutor(), fiberGroup);
        CompletableFuture<Void> jdkFuture = new CompletableFuture<>();
        fiberGroup.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(statusFile.init(), this::afterInit);
            }

            private FrameCallResult afterInit(Void unused) {
                statusFile.getProperties().putAll(data);
                return statusFile.update(true).await(this::afterWrite);
            }

            private FrameCallResult afterWrite(Void unused) {
                jdkFuture.complete(null);
                return Fiber.frameReturn();
            }

            @Override
            protected FrameCallResult doFinally() {
                statusFile.close();
                return super.doFinally();
            }
        });
        jdkFuture.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testUpdateAndInit() throws Exception {
        File dir = TestDir.createTestDir(StatusFileTest.class.getSimpleName());
        File file = new File(dir, "status");
        Properties props = new Properties();
        props.setProperty("1", "100");
        props.setProperty("2", "200");
        update(file, props);
        {
            StatusFile statusFile = new StatusFile(file, MockExecutors.ioExecutor(), fiberGroup);
            CompletableFuture<Void> jdkFuture = new CompletableFuture<>();
            fiberGroup.fireFiber("f", new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    return Fiber.call(statusFile.init(), this::afterInit);
                }

                private FrameCallResult afterInit(Void unused) {
                    assertEquals("100", statusFile.getProperties().getProperty("1"));
                    assertEquals("200", statusFile.getProperties().getProperty("2"));
                    statusFile.getProperties().setProperty("3", "300");
                    return statusFile.update(true).await(this::afterWrite);
                }

                private FrameCallResult afterWrite(Void unused) {
                    jdkFuture.complete(null);
                    return Fiber.frameReturn();
                }

                @Override
                protected FrameCallResult doFinally() {
                    statusFile.close();
                    return super.doFinally();
                }
            });
            jdkFuture.get(1, TimeUnit.SECONDS);
        }
        {
            StatusFile statusFile = new StatusFile(file, MockExecutors.ioExecutor(), fiberGroup);
            CompletableFuture<Void> jdkFuture = new CompletableFuture<>();
            fiberGroup.fireFiber("f", new FiberFrame<>() {
                @Override
                public FrameCallResult execute(Void input) {
                    return Fiber.call(statusFile.init(), this::afterInit);
                }
                private FrameCallResult afterInit(Void unused) {
                    assertEquals("100", statusFile.getProperties().getProperty("1"));
                    assertEquals("200", statusFile.getProperties().getProperty("2"));
                    assertEquals("300", statusFile.getProperties().getProperty("3"));
                    jdkFuture.complete(null);
                    return Fiber.frameReturn();
                }
                @Override
                protected FrameCallResult doFinally() {
                    statusFile.close();
                    return super.doFinally();
                }
            });
            jdkFuture.get(1, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testChecksumError() throws Exception {
        File dir = TestDir.createTestDir(StatusFileTest.class.getSimpleName());
        File file = new File(dir, "status");
        Properties props = new Properties();
        props.setProperty("1", "100");
        props.setProperty("2", "200");
        update(file, props);

        FileInputStream in = new FileInputStream(file);
        byte[] bs = in.readAllBytes();
        in.close();

        // shift crc value
        byte b0 = bs[0];
        for (int i = 0; i < 7; i++) {
            bs[i] = bs[i + 1];
        }
        bs[7] = b0;

        FileOutputStream fos = new FileOutputStream(file);
        fos.write(bs);
        fos.close();

        StatusFile statusFile = new StatusFile(file, MockExecutors.ioExecutor(), fiberGroup);
        CompletableFuture<Throwable> jdkFuture = new CompletableFuture<>();
        fiberGroup.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(statusFile.init(), this::justReturn);
            }
            @Override
            protected FrameCallResult handle(Throwable ex) {
                jdkFuture.complete(ex);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                statusFile.close();
                return super.doFinally();
            }
        });
        assertEquals(ChecksumException.class, DtUtil.rootCause(jdkFuture.get(1, TimeUnit.SECONDS)).getClass());
    }

    @Test
    public void testFileLengthError() throws Exception {
        File dir = TestDir.createTestDir(StatusFileTest.class.getSimpleName());
        File file = new File(dir, "status");
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(1);
        raf.close();
        StatusFile statusFile = new StatusFile(file, MockExecutors.ioExecutor(), fiberGroup);
        CompletableFuture<Throwable> jdkFuture = new CompletableFuture<>();
        fiberGroup.fireFiber("f", new FiberFrame<>() {
            @Override
            public FrameCallResult execute(Void input) {
                return Fiber.call(statusFile.init(), this::justReturn);
            }
            @Override
            protected FrameCallResult handle(Throwable ex) {
                jdkFuture.complete(ex);
                return Fiber.frameReturn();
            }
            @Override
            protected FrameCallResult doFinally() {
                statusFile.close();
                return super.doFinally();
            }
        });
        assertEquals(RaftException.class, jdkFuture.get(1, TimeUnit.SECONDS).getClass());
    }
}

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
package com.github.dtprj.dongting.dtkv;

/**
 * @author huangli
 */
class KvStatus {
    public static final int RUNNING = 0;
    public static final int INSTALLING_SNAPSHOT = 1;
    public static final int CLOSED = 2;

    final int status;
    final KvImpl kvImpl;
    final int epoch;

    public KvStatus(int status, KvImpl kvImpl, int epoch) {
        this.status = status;
        this.kvImpl = kvImpl;
        this.epoch = epoch;
    }
}

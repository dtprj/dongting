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
public class KvClientConfig {

    public static final byte SEPARATOR = '.';
    public static final int MAX_KEY_SIZE = 8 * 1024;
    public static final int MAX_VALUE_SIZE = 1024 * 1024;

    public long watchHeartbeatMillis = 60_000; // // should less than KvServerConfig.watchTimeoutMillis

    // spread mass watch re-register after a server crash, watch recovery may delay up to this value;
    // should be much less than watchHeartbeatMillis; <= 0 disables
    public long watchReRegisterJitterMillis = 5_000;

    public long[] autoRenewalRetryMillis = {1000, 10_000, 30_000, 60_000};
}

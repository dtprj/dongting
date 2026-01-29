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
module dongting.client {
    requires static java.logging;
    requires static org.slf4j;
    requires jdk.unsupported;
    exports com.github.dtprj.dongting.buf;
    exports com.github.dtprj.dongting.common;
    exports com.github.dtprj.dongting.dtkv;
    exports com.github.dtprj.dongting.log;
    exports com.github.dtprj.dongting.net;
    exports com.github.dtprj.dongting.codec;
    exports com.github.dtprj.dongting.raft;
    exports com.github.dtprj.dongting.queue to dongting.server;
    opens com.github.dtprj.dongting.net to dongting.it.test;
}
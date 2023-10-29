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
package com.github.dtprj.dongting.fiber;

/**
 * @author huangli
 */
public class Event {
    final Condition c;
    final boolean signalAll;
    final Object attachment;
    final Runnable runnable;

    public Event(Condition c) {
        this(c, false);
    }

    public Event(Condition c, boolean signalAll) {
        this(c, signalAll, null);
    }

    public Event(Condition c, boolean signalAll, Object attachment) {
        this.c = c;
        this.signalAll = signalAll;
        this.attachment = attachment;
        this.runnable = null;
    }

    Event(Runnable runnable) {
        this.c = null;
        this.signalAll = false;
        this.attachment = null;
        this.runnable = runnable;
    }
}

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

import com.github.dtprj.dongting.raft.RaftException;

/**
 * @author huangli
 */
public class KvException extends RaftException {
    private static final long serialVersionUID = 3325454508293450981L;

    private final int code;

    public KvException(int code) {
        super(KvCodes.toStr(code));
        this.code = code;
    }

    public KvException(int code, Throwable cause) {
        super(KvCodes.toStr(code), cause);
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}

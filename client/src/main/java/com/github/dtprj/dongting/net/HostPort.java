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
package com.github.dtprj.dongting.net;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author huangli
 */
public class HostPort implements Serializable {
    private static final long serialVersionUID = 6380309910720799448L;

    private final String host;
    private final int port;
    private String toStr;

    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        if (toStr == null) {
            toStr = "[" + host + "," + port + "]";
        }
        return toStr;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HostPort) {
            HostPort o = (HostPort) obj;
            return Objects.equals(o.host, this.host) && o.port == this.port;
        }
        return false;
    }
}

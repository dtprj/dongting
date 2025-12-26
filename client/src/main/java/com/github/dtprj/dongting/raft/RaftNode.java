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
package com.github.dtprj.dongting.raft;

import com.github.dtprj.dongting.codec.CodecException;
import com.github.dtprj.dongting.codec.Encodable;
import com.github.dtprj.dongting.codec.EncodeContext;
import com.github.dtprj.dongting.codec.EncodeUtil;
import com.github.dtprj.dongting.codec.PbCallback;
import com.github.dtprj.dongting.codec.PbUtil;
import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.net.NioNet;
import com.github.dtprj.dongting.net.Peer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * @author huangli
 */
public class RaftNode implements Encodable {

    private static final int IDX_NODE_ID = 1;
    private static final int IDX_HOST = 2;
    private static final int IDX_PORT = 3;

    public final int nodeId;
    public final HostPort hostPort;
    public final Peer peer;

    public int useCount;

    private String str;

    public RaftNode(int nodeId, HostPort hostPort) {
        this(nodeId, hostPort, null);
    }

    protected RaftNode(int nodeId, HostPort hostPort, Peer peer) {
        this.nodeId = nodeId;
        this.hostPort = hostPort;
        this.peer = peer;
    }

    @Override
    public String toString() {
        if (str == null) {
            str = nodeId + "@" + hostPort;
        }
        return str;
    }

    public static List<RaftNode> parseServers(String serversStr) {
        if (serversStr == null || serversStr.isEmpty()) {
            //noinspection unchecked
            return Collections.EMPTY_LIST;
        }
        String[] servers = serversStr.split(";");
        if (servers.length == 0) {
            throw new RaftException("servers list is empty");
        }
        try {
            List<RaftNode> list = new ArrayList<>();
            for (String server : servers) {
                String[] arr = server.split(",");
                if (arr.length != 2) {
                    throw new IllegalArgumentException("not 'id,host:port' format:" + server);
                }
                int id = Integer.parseInt(arr[0].trim());
                String hostPortStr = arr[1];
                list.add(new RaftNode(id, NioNet.parseHostPort(hostPortStr)));
            }
            return list;
        } catch (NumberFormatException e) {
            throw new RaftException("bad servers list: " + serversStr);
        }
    }

    public static String formatServers(List<RaftNode> servers) {
        return formatServers(servers, Function.identity());
    }

    public static <T> String formatServers(List<T> servers, Function<T, ? extends RaftNode> mapper) {
        StringBuilder sb = new StringBuilder();
        for (T s : servers) {
            RaftNode n = mapper.apply(s);
            HostPort hp = n.hostPort;
            sb.append(n.nodeId).append(",").append(hp.getHost()).append(":").append(hp.getPort()).append(";");
        }
        return sb.toString();
    }


    @Override
    public boolean encode(EncodeContext context, ByteBuffer destBuffer) {
        switch (context.stage) {
            case EncodeContext.STAGE_BEGIN:
                if (!EncodeUtil.encodeInt32(context, destBuffer, IDX_NODE_ID, nodeId)) {
                    return false;
                }
                // fall through
            case IDX_NODE_ID:
                if (!EncodeUtil.encodeUTF8(context, destBuffer, IDX_HOST, hostPort.getHost())) {
                    return false;
                }
                // fall through
            case IDX_HOST:
                return EncodeUtil.encodeInt32(context, destBuffer, IDX_PORT, hostPort.getPort());
            default:
                throw new CodecException(context);
        }
    }

    @Override
    public int actualSize() {
        return PbUtil.sizeOfInt32Field(IDX_NODE_ID, nodeId)
                + PbUtil.sizeOfUTF8(IDX_HOST, hostPort.getHost())
                + PbUtil.sizeOfInt32Field(IDX_PORT, hostPort.getPort());
    }

    public static class Callback extends PbCallback<RaftNode> {

        private int nodeId;
        private String host;
        private int port;

        @Override
        public boolean readVarNumber(int index, long value) {
            if (index == IDX_NODE_ID) {
                nodeId = (int) value;
            } else if (index == IDX_PORT) {
                port = (int) value;
            }
            return true;
        }

        @Override
        public boolean readBytes(int index, ByteBuffer buf, int fieldLen, int currentPos) {
            if (index == IDX_NODE_ID) {
                host = parseUTF8(buf, fieldLen, currentPos);
            }
            return true;
        }

        @Override
        protected RaftNode getResult() {
            return new RaftNode(nodeId, new HostPort(host, port));
        }

        @Override
        protected void end(boolean success) {
            this.nodeId = 0;
            this.host = null;
            this.port = 0;
        }
    }

}

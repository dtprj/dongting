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
package com.github.dtprj.dongting.it.support;

import com.github.dtprj.dongting.net.HostPort;
import com.github.dtprj.dongting.raft.RaftNode;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author huangli
 */
public class ItUtil {
    private static final int BASE_REPLICATE_PORT = 7700;
    private static final int BASE_SERVICE_PORT = 7701;
    private static final int PORT_STEP = 100;

    public static int replicatePort(int nodeId) {
        return BASE_REPLICATE_PORT + (nodeId - 1) * PORT_STEP;
    }

    public static int servicePort(int nodeId) {
        return BASE_SERVICE_PORT + (nodeId - 1) * PORT_STEP;
    }

    public static String formatReplicateServers(int[] nodeIds) {
        List<RaftNode> replicateNodes = new ArrayList<>();
        for (int nid : nodeIds) {
            int replicatePort = ItUtil.replicatePort(nid);
            RaftNode rn = new RaftNode(nid, new HostPort("127.0.0.1", replicatePort));
            replicateNodes.add(rn);
        }
        return RaftNode.formatServers(replicateNodes);
    }

    public static File findDistDir() {
        File f = new File("").getAbsoluteFile();
        File projectRoot;
        if (f.getName().equals("it-test")) {
            projectRoot = f.getParentFile();
        } else {
            projectRoot = f;
        }
        File result = new File(projectRoot.getAbsolutePath() + File.separator
                + "target" + File.separator + "dongting-dist");
        if (!result.exists() || !result.isDirectory()) {
            throw new RuntimeException("can't find dongting-dist dir");
        }
        return result;
    }
}

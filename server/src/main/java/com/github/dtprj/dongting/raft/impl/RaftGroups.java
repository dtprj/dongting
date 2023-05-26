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
package com.github.dtprj.dongting.raft.impl;

import com.github.dtprj.dongting.common.IntObjMap;

/**
 * @author huangli
 */
public class RaftGroups {
    private volatile IntObjMap<RaftGroupImpl<?, ?, ?>> map = new IntObjMap<>();

    public RaftGroupImpl get(int groupId) {
        return map.get(groupId);
    }

    public void put(int groupId, RaftGroupImpl<?, ?, ?> raftGroupImpl) {
        IntObjMap<RaftGroupImpl<?, ?, ?>> newMap = new IntObjMap<>();
        map.forEach((k, v) -> {
            newMap.put(k, v);
            return true;
        });
        newMap.put(groupId, raftGroupImpl);
        this.map = newMap;
    }

    public void forEach(IntObjMap.Visitor<RaftGroupImpl<?, ?, ?>> visitor) {
        map.forEach(visitor);
    }

    public void remove(int groupId) {
        map.remove(groupId);
    }
}

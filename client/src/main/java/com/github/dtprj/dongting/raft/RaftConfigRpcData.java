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

import java.util.HashSet;
import java.util.Set;

/**
 * @author huangli
 */
public class RaftConfigRpcData extends RaftRpcData {
    public Set<Integer> members = new HashSet<>();
    public Set<Integer> observers = new HashSet<>();
    public Set<Integer> preparedMembers = new HashSet<>();
    public Set<Integer> preparedObservers = new HashSet<>();

    public Set<Integer> getMembers() {
        return members;
    }

    public void setMembers(Set<Integer> members) {
        this.members = members;
    }

    public Set<Integer> getObservers() {
        return observers;
    }

    public void setObservers(Set<Integer> observers) {
        this.observers = observers;
    }

    public Set<Integer> getPreparedMembers() {
        return preparedMembers;
    }

    public void setPreparedMembers(Set<Integer> preparedMembers) {
        this.preparedMembers = preparedMembers;
    }

    public Set<Integer> getPreparedObservers() {
        return preparedObservers;
    }

    public void setPreparedObservers(Set<Integer> preparedObservers) {
        this.preparedObservers = preparedObservers;
    }
}

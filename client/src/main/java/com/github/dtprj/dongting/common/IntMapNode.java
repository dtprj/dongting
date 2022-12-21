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
package com.github.dtprj.dongting.common;

/**
 * @author huangli
 */
class IntMapNode<V> {
    @SuppressWarnings("FieldMayBeFinal")
    private int key;
    private V value;
    private IntMapNode<V> next;

    public IntMapNode(int key, V value) {
        this.key = key;
        this.value = value;
    }

    public int getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public IntMapNode<V> getNext() {
        return next;
    }

    public void setNext(IntMapNode<V> next) {
        this.next = next;
    }

    public void setValue(V value) {
        this.value = value;
    }
}

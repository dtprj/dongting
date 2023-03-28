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
package com.github.dtprj.dongting.raft.file;

/**
 * @author huangli
 */
class IndexedDeque<T> {
    private Object[] elements;
    private int readIndex;
    private int writeIndex;
    private int size;
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    public IndexedDeque() {
        elements = new Object[DEFAULT_INITIAL_CAPACITY];
    }

    public void addLast(T element) {
        ensureCapacity();
        elements[writeIndex] = element;
        writeIndex = (writeIndex + 1) % elements.length;
        size++;
    }

    @SuppressWarnings("unchecked")
    public T removeFirst() {
        if (size == 0) {
            return null;
        }

        T element = (T) elements[readIndex];
        elements[readIndex] = null;
        readIndex = (readIndex + 1) % elements.length;
        size--;
        return element;
    }

    @SuppressWarnings("unchecked")
    public T get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index out of range");
        }

        int position = (readIndex + index) % elements.length;
        return (T) elements[position];
    }

    public int size() {
        return size;
    }

    private void ensureCapacity() {
        if (size == elements.length) {
            int newSize = elements.length * 2;
            Object[] newElements = new Object[newSize];

            int firstPartSize = elements.length - readIndex;
            int secondPartSize = writeIndex;

            System.arraycopy(elements, readIndex, newElements, 0, firstPartSize);
            System.arraycopy(elements, 0, newElements, firstPartSize, secondPartSize);

            elements = newElements;
            readIndex = 0;
            writeIndex = size;
        }
    }
}


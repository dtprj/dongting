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
@SuppressWarnings("unchecked")
public class IndexedQueue<T> {
    protected Object[] elements;
    protected int len;
    protected int mask;
    protected int readIndex;
    protected int writeIndex;
    protected int size;

    public IndexedQueue(int initialCapacity) {
        len = BitUtil.nextHighestPowerOfTwo(initialCapacity);
        mask = len - 1;
        elements = new Object[len];
    }

    public void addFirst(T element) {
        ensureCapacity();
        int index = readIndex - 1;
        if (index < 0) {
            index = len - 1;
        }
        elements[index] = element;
        readIndex = index;
        size++;
    }

    public void addLast(T element) {
        ensureCapacity();
        elements[writeIndex] = element;
        writeIndex = (writeIndex + 1) & mask;
        size++;
    }

    public T removeFirst() {
        if (size == 0) {
            return null;
        }

        T element = (T) elements[readIndex];
        elements[readIndex] = null;
        readIndex = (readIndex + 1) & mask;
        size--;
        return element;
    }

    public T removeLast() {
        if (size == 0) {
            return null;
        }
        int index = writeIndex - 1;
        if (index < 0) {
            index = len - 1;
        }
        T element = (T) elements[index];
        elements[index] = null;
        writeIndex = index;
        size--;
        return element;
    }

    public T get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index out of range");
        }

        int position = (readIndex + index) & mask;
        return (T) elements[position];
    }

    public int size() {
        return size;
    }

    private void ensureCapacity() {
        if (size == len) {
            int newSize = len << 1;
            Object[] newElements = new Object[newSize];

            int firstPartSize = len - readIndex;
            int secondPartSize = writeIndex;

            System.arraycopy(elements, readIndex, newElements, 0, firstPartSize);
            System.arraycopy(elements, 0, newElements, firstPartSize, secondPartSize);

            elements = newElements;
            len = newSize;
            mask = len - 1;
            readIndex = 0;
            writeIndex = size;
        }
    }
}


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
package com.github.dtprj.dongting.dtkv.server;

import com.github.dtprj.dongting.common.ByteArray;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class KvServerUtilTest {

    @Test
    public void testBuildAndParseLockKey() {
        ByteArray parent = new ByteArray("test.lock".getBytes());
        UUID uuid = UUID.randomUUID();
        long uuid1 = uuid.getMostSignificantBits();
        long uuid2 = uuid.getLeastSignificantBits();
        
        // Test build lock key
        ByteArray lockKey = KvServerUtil.buildLockKey(parent, uuid1, uuid2);
        assertNotNull(lockKey);
        
        // Test parse lock key
        UUID parsedUuid = KvServerUtil.parseLockKeyUuid(lockKey);
        assertNotNull(parsedUuid);
        assertEquals(uuid, parsedUuid);
        
        // Test with empty parent path
        ByteArray emptyParent = new ByteArray("".getBytes());
        ByteArray emptyParentKey = KvServerUtil.buildLockKey(emptyParent, uuid1, uuid2);
        UUID emptyParentUuid = KvServerUtil.parseLockKeyUuid(emptyParentKey);
        assertEquals(uuid, emptyParentUuid);
        
        // Test with single char parent
        ByteArray singleCharParent = new ByteArray("a".getBytes());
        ByteArray singleCharKey = KvServerUtil.buildLockKey(singleCharParent, uuid1, uuid2);
        UUID singleCharUuid = KvServerUtil.parseLockKeyUuid(singleCharKey);
        assertEquals(uuid, singleCharUuid);
    }
    
    @Test
    public void testParseLockKeyInvalidInput() {
        // Test with short key
        ByteArray shortKey = new ByteArray("short".getBytes());
        UUID result = KvServerUtil.parseLockKeyUuid(shortKey);
        assertNull(result);
        
        // Test with separator not at expected position
        String wrongSeparatorPos = "test" + "/" + "0123456789abcdef" + "0123456789abcdef";
        ByteArray wrongSepKey = new ByteArray(wrongSeparatorPos.getBytes());
        result = KvServerUtil.parseLockKeyUuid(wrongSepKey);
        assertNull(result);
        
        // Test with invalid hex chars
        String invalidHex = "test" + "/" + "g123456789abcdef" + "0123456789abcdef";
        ByteArray invalidKey = new ByteArray(invalidHex.getBytes());
        result = KvServerUtil.parseLockKeyUuid(invalidKey);
        assertNull(result);
        
        // Test with uppercase hex chars
        String upperHex = "test" + "/" + "0123456789ABCDEF" + "0123456789abcdef";
        ByteArray upperKey = new ByteArray(upperHex.getBytes());
        result = KvServerUtil.parseLockKeyUuid(upperKey);
        assertNull(result);
        
        // Test with exactly 32 bytes but no separator
        String noSeparator = "test" + "0123456789abcdef" + "0123456789abcdef";
        ByteArray noSepKey = new ByteArray(noSeparator.getBytes());
        result = KvServerUtil.parseLockKeyUuid(noSepKey);
        assertNull(result);
    }
}
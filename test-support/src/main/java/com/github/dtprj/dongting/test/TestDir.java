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
package com.github.dtprj.dongting.test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author huangli
 */
public class TestDir {
    private static final String TEST_DIR = "target/test-data";
    private static final SimpleDateFormat sdf = new SimpleDateFormat("_HHmmss_SSS_");

    public static File createTestDirWithoutSuffix(String dirName) {
        File dir = new File(new File(TEST_DIR), dirName);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("create dir fail");
            }
        }
        return dir;
    }

    public static File createTestDir(String prefix) {
        File dir = new File(TEST_DIR);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("create dir fail");
            }
        }
        String s = sdf.format(new Date());
        int x = 0;
        while (true) {
            String name = prefix + s + x;
            File subDir = new File(dir, name);
            if (!subDir.exists()) {
                if (!subDir.mkdirs()) {
                    throw new RuntimeException("create dir fail");
                }
                return subDir;
            } else {
                x++;
            }
        }
    }

    public static String testDir(String prefix) {
        String s = sdf.format(new Date());
        int x = 0;
        while (true) {
            String result = TEST_DIR + "/" + prefix + s + x;
            File subDir = new File(result);
            if (!subDir.exists()) {
                return result;
            } else {
                x++;
            }
        }
    }
}

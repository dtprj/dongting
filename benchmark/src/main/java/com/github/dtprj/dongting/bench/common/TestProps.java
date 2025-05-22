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
package com.github.dtprj.dongting.bench.common;

import java.io.File;
import java.util.Properties;

/**
 * @author huangli
 */
public class TestProps {
    private static final Properties props = new Properties();

    static {
        File home = new File(System.getProperty("user.home"));
        File configFile = new File(home, ".dt-test");
        if (configFile.exists()) {
            try {
                props.load(new java.io.FileInputStream(configFile));
            } catch (Exception e) {
                throw new RuntimeException("load config file error", e);
            }
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }

    public static String get(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }
}

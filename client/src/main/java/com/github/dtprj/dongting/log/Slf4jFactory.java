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
package com.github.dtprj.dongting.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author huangli
 */
public class Slf4jFactory implements DtLogFactory {

    public static boolean slf4jExists() {
        try {
            Class.forName("org.slf4j.Logger");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    public DtLog getLogger(String name) {
        Logger logger = LoggerFactory.getLogger(name);
        return new Slf4jLog(logger);
    }

    @Override
    public DtLog getLogger(Class<?> clazz) {
        Logger logger = LoggerFactory.getLogger(clazz);
        return new Slf4jLog(logger);
    }
}

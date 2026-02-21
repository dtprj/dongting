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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;

/**
 * @author huangli
 */
public class BugLogExtension implements BeforeEachCallback, AfterEachCallback {

    private static final String BUG_LOG_CLASS = "com.github.dtprj.dongting.log.BugLog";

    @Override
    public void beforeEach(ExtensionContext context) {
    }

    @Override
    public void afterEach(ExtensionContext context) {
        int count;
        String firstError;
        try {
            Class<?> bugLogClass = Class.forName(BUG_LOG_CLASS);
            Method getCountMethod = bugLogClass.getMethod("getCount");
            Method getFirstErrorMethod = bugLogClass.getMethod("getFirstError");
            count = (int) getCountMethod.invoke(null);
            firstError = (String) getFirstErrorMethod.invoke(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (count != 0) {
            String message = "BugLog count should be 0, but was " + count + 
                    ". First error: " + (firstError != null ? firstError : "null");
            Assertions.fail(message);
        }
    }
}

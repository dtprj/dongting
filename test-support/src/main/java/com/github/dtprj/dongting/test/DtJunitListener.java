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

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

import java.util.Optional;

/**
 * @author huangli
 */
public class DtJunitListener implements TestExecutionListener {

    private long time;

    @Override
    public void executionStarted(TestIdentifier identifier) {
        if (identifier.isTest()) {
            time = System.currentTimeMillis();
            System.out.println("Starting test method: " + getDisplayName(identifier));
        }
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult result) {
        if (testIdentifier.isTest()) {
            System.out.println("Finished test method: " + getDisplayName(testIdentifier)
                    + " -- Result: " + result.getStatus() + ", Time: " + (System.currentTimeMillis() - time) + "ms");
        }
    }

    private String getDisplayName(TestIdentifier identifier) {
        Optional<TestSource> source = identifier.getSource();
        if (source.isPresent() && source.get() instanceof MethodSource methodSource) {
            String s = methodSource.getJavaClass().getSimpleName() + "." + methodSource.getMethodName() + "()";
            if (!identifier.getDisplayName().contains(methodSource.getMethodName())) {
                // eg: "[1] false" for @ParameterizedTest
                s = s  + " " + identifier.getDisplayName();
            }
            return s;
        } else {
            return identifier.getDisplayName();
        }
    }
}

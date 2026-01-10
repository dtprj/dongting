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
package com.github.dtprj.dongting.dist;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author huangli
 */
public class PropsUtilTest {

    @AfterEach
    public void tearDown() {
        System.clearProperty("testVar");
        System.clearProperty("testVar2");
    }

    // Test bean class with various field types
    public static class TestBean {
        public String stringField;
        public int intField;
        public long longField;
        public boolean booleanField;
        public int[] intArrayField;
        public long[] longArrayField;

        // These fields should be ignored
        @SuppressWarnings("unused")
        public static String staticField;
        @SuppressWarnings("unused")
        public final String finalField = "final";
        @SuppressWarnings("unused")
        private String privateField;
    }

    @Test
    public void testSetFieldsFromProps() {
        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("stringField", "hello");
        props.setProperty("intField", "42");
        props.setProperty("longField", "123456789");
        props.setProperty("booleanField", "true");
        props.setProperty("intArrayField", "1,2,3");
        props.setProperty("longArrayField", "100,200,300");

        PropsUtil.setFieldsFromProps(bean, props, null);

        assertEquals("hello", bean.stringField);
        assertEquals(42, bean.intField);
        assertEquals(123456789L, bean.longField);
        assertTrue(bean.booleanField);
        assertArrayEquals(new int[]{1, 2, 3}, bean.intArrayField);
        assertArrayEquals(new long[]{100, 200, 300}, bean.longArrayField);
    }

    @Test
    public void testSetFieldsWithPrefix() {
        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("prefix.stringField", "world");
        props.setProperty("prefix.intField", "100");

        PropsUtil.setFieldsFromProps(bean, props, "prefix");

        assertEquals("world", bean.stringField);
        assertEquals(100, bean.intField);
    }

    @Test
    public void testBooleanFalse() {
        TestBean bean = new TestBean();
        bean.booleanField = true;
        Properties props = new Properties();
        props.setProperty("booleanField", "false");

        PropsUtil.setFieldsFromProps(bean, props, null);

        assertFalse(bean.booleanField);
    }

    @Test
    public void testBooleanCaseInsensitive() {
        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("booleanField", "TRUE");

        PropsUtil.setFieldsFromProps(bean, props, null);

        assertTrue(bean.booleanField);
    }

    @Test
    public void testInvalidBooleanThrows() {
        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("booleanField", "invalid");

        assertThrows(RuntimeException.class, () ->
                PropsUtil.setFieldsFromProps(bean, props, null));
    }

    @Test
    public void testNullBeanOrProps() {
        Properties props = new Properties();
        TestBean bean = new TestBean();

        // Should not throw
        PropsUtil.setFieldsFromProps(null, props, null);
        PropsUtil.setFieldsFromProps(bean, null, null);
    }

    @Test
    public void testEmptyValueIgnored() {
        TestBean bean = new TestBean();
        bean.stringField = "original";
        Properties props = new Properties();
        props.setProperty("stringField", "   ");

        PropsUtil.setFieldsFromProps(bean, props, null);

        assertEquals("original", bean.stringField);
    }

    @Test
    public void testVariableFromSystemProperty() {
        System.setProperty("testVar", "fromSystem");

        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("stringField", "${testVar}");

        PropsUtil.setFieldsFromProps(bean, props, null);

        assertEquals("fromSystem", bean.stringField);
    }

    @Test
    public void testVariableWithDefault() {
        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("stringField", "${nonExistentVar:defaultValue}");

        PropsUtil.setFieldsFromProps(bean, props, null);

        assertEquals("defaultValue", bean.stringField);
    }

    @Test
    public void testVariableWithEmptyDefault() {
        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("stringField", "${nonExistentVar:}");

        PropsUtil.setFieldsFromProps(bean, props, null);

        assertEquals("", bean.stringField);
    }

    @Test
    public void testMultipleVariables() {
        System.setProperty("testVar", "hello");
        System.setProperty("testVar2", "world");

        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("stringField", "${testVar}-${testVar2}");

        PropsUtil.setFieldsFromProps(bean, props, null);

        assertEquals("hello-world", bean.stringField);
    }

    @Test
    public void testVariableMixedWithText() {
        System.setProperty("testVar", "value");

        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("stringField", "prefix-${testVar}-suffix");

        PropsUtil.setFieldsFromProps(bean, props, null);

        assertEquals("prefix-value-suffix", bean.stringField);
    }

    @Test
    public void testVariableInIntField() {
        System.setProperty("testVar", "999");

        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("intField", "${testVar}");

        PropsUtil.setFieldsFromProps(bean, props, null);

        assertEquals(999, bean.intField);
    }

    @Test
    public void testUnresolvedVariableThrows() {
        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("stringField", "${undefinedVar}");

        assertThrows(RuntimeException.class, () ->
                PropsUtil.setFieldsFromProps(bean, props, null));
    }

    @Test
    public void testNoVariableInValue() {
        TestBean bean = new TestBean();
        Properties props = new Properties();
        props.setProperty("stringField", "plainValue");

        PropsUtil.setFieldsFromProps(bean, props, null);

        assertEquals("plainValue", bean.stringField);
    }

    @Test
    public void testVariableFromEnv() {
        // PATH is a common environment variable on most systems
        String path = System.getenv("PATH");
        if (path != null) {
            TestBean bean = new TestBean();
            Properties props = new Properties();
            props.setProperty("stringField", "${PATH}");

            PropsUtil.setFieldsFromProps(bean, props, null);

            assertEquals(path, bean.stringField);
        }
    }

    @Test
    public void testSystemPropertyPrecedenceOverEnv() {
        // PATH exists in env, set same name in system property
        System.setProperty("PATH", "systemPathValue");
        try {
            TestBean bean = new TestBean();
            Properties props = new Properties();
            props.setProperty("stringField", "${PATH}");

            PropsUtil.setFieldsFromProps(bean, props, null);

            assertEquals("systemPathValue", bean.stringField);
        } finally {
            System.clearProperty("PATH");
        }
    }
}

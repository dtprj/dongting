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
package com.github.dtprj.dongting.boot;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

/**
 * @author huangli
 */
public class PropsUtil {

    /**
     * Set public fields of a bean from properties.
     * Supported field types: String, int, long, boolean, int[], long[].
     *
     * @param bean   the bean object to set fields on
     * @param props  properties containing field values
     * @param prefix prefix for property keys (can be null or empty)
     */
    public static void setFieldsFromProps(Object bean, Properties props, String prefix) {
        if (bean == null || props == null) {
            return;
        }
        if (prefix == null) {
            prefix = "";
        }
        Class<?> clazz = bean.getClass();
        Field[] fields = clazz.getFields();
        for (Field field : fields) {
            int modifiers = field.getModifiers();
            if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers) || Modifier.isFinal(modifiers)) {
                continue;
            }
            String key = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            String value = props.getProperty(key);
            if (value == null) {
                continue;
            }
            value = value.trim();
            if (value.isEmpty()) {
                continue;
            }
            try {
                setField(bean, field, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to set field " + field.getName() + " with value '" + value + "'", e);
            }
        }
    }

    private static void setField(Object bean, Field field, String value) throws IllegalAccessException {
        Class<?> type = field.getType();
        if (type == String.class) {
            field.set(bean, value);
        } else if (type == int.class) {
            field.setInt(bean, Integer.parseInt(value));
        } else if (type == long.class) {
            field.setLong(bean, Long.parseLong(value));
        } else if (type == boolean.class) {
            field.setBoolean(bean, parseBoolean(value));
        } else if (type == int[].class) {
            field.set(bean, parseIntArray(value));
        } else if (type == long[].class) {
            field.set(bean, parseLongArray(value));
        }
    }

    private static boolean parseBoolean(String value) {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        } else if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException("Invalid boolean value: " + value);
    }

    private static int[] parseIntArray(String value) {
        String[] parts = value.split(",");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Integer.parseInt(parts[i].trim());
        }
        return result;
    }

    private static long[] parseLongArray(String value) {
        String[] parts = value.split(",");
        long[] result = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Long.parseLong(parts[i].trim());
        }
        return result;
    }
}

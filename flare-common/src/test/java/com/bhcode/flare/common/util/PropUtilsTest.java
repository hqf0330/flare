/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bhcode.flare.common.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * PropUtils 工具类单元测试
 *
 * @author Flare Team
 * @since 1.0.0
 */
public class PropUtilsTest {

    private Path tempDir;
    private File testPropertiesFile;

    @Before
    public void setUp() throws IOException {
        // 创建临时目录
        tempDir = Files.createTempDirectory("flare-test-");
        testPropertiesFile = new File(tempDir.toFile(), "test.properties");
        
        // 创建测试配置文件
        try (FileWriter writer = new FileWriter(testPropertiesFile)) {
            writer.write("test.string.key=test_value\n");
            writer.write("test.int.key=123\n");
            writer.write("test.long.key=456789\n");
            writer.write("test.boolean.key=true\n");
            writer.write("test.boolean.false.key=false\n");
            writer.write("prefix.key1=value1\n");
            writer.write("prefix.key2=value2\n");
            writer.write("other.key=other_value\n");
        }
    }

    @After
    public void tearDown() throws IOException {
        // 清理临时文件
        if (testPropertiesFile != null && testPropertiesFile.exists()) {
            testPropertiesFile.delete();
        }
        if (tempDir != null && Files.exists(tempDir)) {
            Files.delete(tempDir);
        }
    }

    @Test
    public void testGetString() {
        // 测试获取 String 类型配置
        PropUtils.setProperty("test.string.key", "test_value");
        String value = PropUtils.getString("test.string.key");
        assertEquals("test_value", value);
        
        // 测试不存在的 key
        String nullValue = PropUtils.getString("non.existent.key");
        assertNull(nullValue);
    }

    @Test
    public void testGetStringWithDefault() {
        // 测试带默认值的 getString
        String value = PropUtils.getString("non.existent.key", "default_value");
        assertEquals("default_value", value);
        
        // 测试存在的 key
        PropUtils.setProperty("test.string.key", "test_value");
        String existingValue = PropUtils.getString("test.string.key", "default_value");
        assertEquals("test_value", existingValue);
    }

    @Test
    public void testGetInt() {
        // 测试获取 Int 类型配置
        PropUtils.setProperty("test.int.key", "123");
        int value = PropUtils.getInt("test.int.key", 0);
        assertEquals(123, value);
        
        // 测试不存在的 key，返回默认值
        int defaultValue = PropUtils.getInt("non.existent.key", 999);
        assertEquals(999, defaultValue);
        
        // 测试无效的整数值，应该返回默认值
        PropUtils.setProperty("test.invalid.int", "not_a_number");
        int invalidValue = PropUtils.getInt("test.invalid.int", 999);
        assertEquals(999, invalidValue);
    }

    @Test
    public void testGetLong() {
        // 测试获取 Long 类型配置
        PropUtils.setProperty("test.long.key", "456789");
        long value = PropUtils.getLong("test.long.key", 0L);
        assertEquals(456789L, value);
        
        // 测试不存在的 key，返回默认值
        long defaultValue = PropUtils.getLong("non.existent.key", 999L);
        assertEquals(999L, defaultValue);
    }

    @Test
    public void testGetBoolean() {
        // 测试获取 Boolean 类型配置
        PropUtils.setProperty("test.boolean.key", "true");
        boolean value = PropUtils.getBoolean("test.boolean.key", false);
        assertTrue(value);
        
        PropUtils.setProperty("test.boolean.false.key", "false");
        boolean falseValue = PropUtils.getBoolean("test.boolean.false.key", true);
        assertFalse(falseValue);
        
        // 测试不存在的 key，返回默认值
        boolean defaultValue = PropUtils.getBoolean("non.existent.key", true);
        assertTrue(defaultValue);
    }

    @Test
    public void testSliceKeys() {
        // 设置测试数据
        PropUtils.setProperty("prefix.key1", "value1");
        PropUtils.setProperty("prefix.key2", "value2");
        PropUtils.setProperty("other.key", "other_value");
        
        // 测试按前缀过滤
        Map<String, String> prefixMap = PropUtils.sliceKeys("prefix.");
        assertEquals(2, prefixMap.size());
        assertEquals("value1", prefixMap.get("key1"));
        assertEquals("value2", prefixMap.get("key2"));
        
        // 测试空前缀（应该返回所有配置，因为所有 key 都以 "" 开头）
        Map<String, String> allMap = PropUtils.sliceKeys("");
        // 空前缀会匹配所有 key，所以不应该为空（至少包含我们设置的 3 个 key）
        assertTrue(allMap.size() >= 3);
        
        // 测试不存在的前缀
        Map<String, String> nonExistentMap = PropUtils.sliceKeys("nonexistent.");
        assertTrue(nonExistentMap.isEmpty());
    }

    @Test
    public void testSetProperty() {
        // 测试设置属性
        PropUtils.setProperty("new.key", "new_value");
        String value = PropUtils.getString("new.key");
        assertEquals("new_value", value);
        
        // 测试覆盖已存在的属性
        PropUtils.setProperty("new.key", "updated_value");
        String updatedValue = PropUtils.getString("new.key");
        assertEquals("updated_value", updatedValue);
    }

    @Test
    public void testGetSettings() {
        // 设置一些测试数据
        PropUtils.setProperty("key1", "value1");
        PropUtils.setProperty("key2", "value2");
        
        // 获取所有配置
        Map<String, String> settings = PropUtils.getSettings();
        assertNotNull(settings);
        assertTrue(settings.size() > 0);
        
        // 验证配置项存在
        assertTrue(settings.containsKey("key1"));
        assertTrue(settings.containsKey("key2"));
        assertEquals("value1", settings.get("key1"));
        assertEquals("value2", settings.get("key2"));
    }
}

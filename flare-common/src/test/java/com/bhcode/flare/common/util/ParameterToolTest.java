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

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * ParameterTool 工具类单元测试
 *
 * @author Flare Team
 * @since 1.0.0
 */
public class ParameterToolTest {

    @Test
    public void testFromArgs() {
        // 测试标准参数解析
        String[] args = {"--key1", "value1", "--key2", "value2", "-key3", "value3"};
        ParameterTool tool = ParameterTool.fromArgs(args);
        
        assertEquals("value1", tool.get("key1"));
        assertEquals("value2", tool.get("key2"));
        assertEquals("value3", tool.get("key3"));
    }

    @Test
    public void testFromArgsWithShortKeys() {
        // 测试短参数（单个 -）
        String[] args = {"-k", "v", "--long", "value"};
        ParameterTool tool = ParameterTool.fromArgs(args);
        
        assertEquals("v", tool.get("k"));
        assertEquals("value", tool.get("long"));
    }

    @Test
    public void testFromArgsWithNoValue() {
        // 测试没有值的参数
        String[] args = {"--flag", "--key", "value"};
        ParameterTool tool = ParameterTool.fromArgs(args);
        
        assertNull(tool.get("flag")); // 没有值的参数返回 null
        assertEquals("value", tool.get("key"));
    }

    @Test
    public void testGetWithDefault() {
        // 测试带默认值的 get 方法
        String[] args = {"--key1", "value1"};
        ParameterTool tool = ParameterTool.fromArgs(args);
        
        assertEquals("value1", tool.get("key1", "default"));
        assertEquals("default", tool.get("nonExistent", "default"));
    }

    @Test
    public void testHas() {
        // 测试 has 方法
        String[] args = {"--key1", "value1", "--key2"};
        ParameterTool tool = ParameterTool.fromArgs(args);
        
        assertTrue(tool.has("key1"));
        assertTrue(tool.has("key2"));
        assertFalse(tool.has("nonExistent"));
    }

    @Test
    public void testToMap() {
        // 测试转换为 Map
        String[] args = {"--key1", "value1", "--key2", "value2"};
        ParameterTool tool = ParameterTool.fromArgs(args);
        
        Map<String, String> map = tool.toMap();
        assertEquals(2, map.size());
        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));
    }

    @Test
    public void testEmptyArgs() {
        // 测试空参数
        String[] args = {};
        ParameterTool tool = ParameterTool.fromArgs(args);
        
        assertTrue(tool.toMap().isEmpty());
        assertFalse(tool.has("any"));
        assertNull(tool.get("any"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidArgs() {
        // 测试无效参数（不以 - 或 -- 开头）
        String[] args = {"invalid", "value"};
        ParameterTool.fromArgs(args);
    }
}

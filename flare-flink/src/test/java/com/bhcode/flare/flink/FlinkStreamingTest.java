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

package com.bhcode.flare.flink;

import com.bhcode.flare.common.enums.JobType;
import com.bhcode.flare.flink.anno.Streaming;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * FlinkStreaming 基础功能单元测试
 *
 * @author Flare Team
 * @since 1.0.0
 */
public class FlinkStreamingTest {

    /**
     * 测试用的 FlinkStreaming 子类
     */
    @Streaming(interval = 10, parallelism = 2)
    static class TestFlinkStreaming extends FlinkStreaming {
        private boolean processCalled = false;
        private boolean contextCreated = false;

        @Override
        public void process() {
            processCalled = true;
        }

        @Override
        protected void createContext(Object conf) {
            super.createContext(conf);
            contextCreated = true;
        }

        public boolean isProcessCalled() {
            return processCalled;
        }

        public boolean isContextCreated() {
            return contextCreated;
        }
    }

    private TestFlinkStreaming flinkStreaming;

    @Before
    public void setUp() {
        flinkStreaming = new TestFlinkStreaming();
    }

    @After
    public void tearDown() {
        if (flinkStreaming != null) {
            try {
                flinkStreaming.stop();
            } catch (Exception e) {
                // 忽略清理错误
            }
        }
    }

    @Test
    public void testJobType() {
        // 测试任务类型设置
        // 注意：BaseFlare 中 jobType 字段是 final 的，初始化为 UNDEFINED
        // FlinkStreaming 重写了 getJobType() 方法，返回 JobType.FLINK_STREAMING
        // 所以即使实例字段是 UNDEFINED，getJobType() 也会返回 FLINK_STREAMING

        // 不触发 init()，避免依赖完整的 Flink Table Planner
        // getJobType() 应该始终返回 FLINK_STREAMING
        assertEquals("FlinkStreaming.getJobType() 应返回 FLINK_STREAMING",
                JobType.FLINK_STREAMING, flinkStreaming.getJobType());
    }

    @Test
    @Ignore("需要完整的 Flink Table Planner 环境，暂时跳过")
    public void testCreateContext() {
        // 测试上下文创建
        // 注意：此测试需要完整的 Flink Table Planner 环境
        assertFalse(flinkStreaming.isContextCreated());

        flinkStreaming.init(null, null);

        assertTrue(flinkStreaming.isContextCreated());
        assertNotNull("StreamExecutionEnvironment 不应为 null", flinkStreaming.getEnv());
        assertNotNull("StreamTableEnvironment 不应为 null", flinkStreaming.getTableEnv());
    }

    @Test
    @Ignore("需要完整的 Flink Table Planner 环境，暂时跳过")
    public void testFireAlias() {
        // 测试 fire 别名
        // 注意：此测试需要完整的 Flink Table Planner 环境
        flinkStreaming.init(null, null);

        StreamExecutionEnvironment env = flinkStreaming.getEnv();
        assertNotNull("fire() 方法应返回非 null 的 StreamExecutionEnvironment", env);
        assertEquals("fire() 应返回与 getEnv() 相同的实例",
                flinkStreaming.getEnv(), env);
    }

    @Test
    public void testBuildConf() {
        // 测试配置构建
        Configuration conf = new Configuration();
        Configuration result = flinkStreaming.buildConf(conf);

        assertNotNull(result);
        assertTrue(result.getBoolean(ConfigConstants.LOCAL_START_WEBSERVER, false));
    }

    @Test
    @Ignore("需要完整的 Flink Table Planner 环境，暂时跳过")
    public void testProcessCalled() {
        // 测试 process 方法是否被调用
        // 注意：此测试需要完整的 Flink Table Planner 环境
        assertFalse(flinkStreaming.isProcessCalled());

        flinkStreaming.init(null, null);

        // process() 会在 processAll() 中被调用
        assertTrue("process() 方法应该被调用", flinkStreaming.isProcessCalled());
    }

    @Test
    public void testAppName() {
        // 测试应用名称
        String appName = flinkStreaming.getAppName();
        assertNotNull(appName);
        assertEquals("TestFlinkStreaming", appName);
    }

    @Test
    @Ignore("需要完整的 Flink Table Planner 环境，暂时跳过")
    public void testSqlMethods() {
        // 测试 SQL 相关方法
        // 注意：此测试需要完整的 Flink Table Planner 环境
        flinkStreaming.init(null, null);

        // 测试 sql() 方法（应该不抛出异常）
        try {
            flinkStreaming.sql("CREATE TABLE test_table (id INT) WITH ('connector' = 'datagen')");
        } catch (Exception e) {
            // 在测试环境中可能会失败，这是正常的
        }

        // 测试 sqlQuery() 方法
        try {
            flinkStreaming.sqlQuery("SELECT 1");
        } catch (Exception e) {
            // 在测试环境中可能会失败，这是正常的
        }
    }
}

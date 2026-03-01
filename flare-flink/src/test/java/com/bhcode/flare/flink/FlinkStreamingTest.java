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
import com.bhcode.flare.flink.conf.FlareFlinkConf;
import com.bhcode.flare.common.util.PropUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
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
        // 当前项目范围：默认不启用 TableEnvironment（Flink SQL 不在本阶段范围内）
        PropUtils.setProperty(FlareFlinkConf.FLINK_TABLE_ENV_ENABLE, "false");
        // 默认开启自动启动，单测按需覆盖
        PropUtils.setProperty(FlareFlinkConf.FLINK_JOB_AUTO_START, "true");
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
    public void testCreateContext() {
        // 在不启用 TableEnvironment 的默认范围下，仍需正确创建 StreamExecutionEnvironment
        assertFalse(flinkStreaming.isContextCreated());

        flinkStreaming.createContext(new Configuration());

        assertTrue(flinkStreaming.isContextCreated());
        assertNotNull("StreamExecutionEnvironment 不应为 null", flinkStreaming.getEnv());
        assertNull("默认应不初始化 StreamTableEnvironment", flinkStreaming.getTableEnv());
    }

    @Test
    public void testFireAlias() {
        // 在 createContext 后，getEnv() 应可直接使用
        flinkStreaming.createContext(new Configuration());

        StreamExecutionEnvironment env = flinkStreaming.getEnv();
        assertNotNull("getEnv() 应返回非 null 的 StreamExecutionEnvironment", env);
        assertEquals("同一次创建应返回同一个 env 实例",
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
    public void testProcessCalled() {
        // 关闭 auto start，避免触发 env.execute，专注验证 init -> processAll 链路
        PropUtils.setProperty(FlareFlinkConf.FLINK_JOB_AUTO_START, "false");
        assertFalse(flinkStreaming.isProcessCalled());

        flinkStreaming.init(new Configuration(), null);

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
    public void testSqlMethods() {
        // 当前范围下默认不初始化 TableEnvironment，SQL 入口应给出清晰错误
        flinkStreaming.createContext(new Configuration());

        assertThrows(IllegalStateException.class, () -> flinkStreaming.sql("SELECT 1"));
        assertThrows(IllegalStateException.class, () -> flinkStreaming.sqlQuery("SELECT 1"));
    }
}

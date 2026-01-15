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

package com.bhcode.flare.core;

import com.bhcode.flare.common.enums.JobType;
import com.bhcode.flare.core.anno.connector.Process;
import com.bhcode.flare.core.anno.connector.After;
import com.bhcode.flare.core.anno.connector.Before;
import com.bhcode.flare.core.anno.lifecycle.Step1;
import com.bhcode.flare.core.anno.lifecycle.Step2;
import com.bhcode.flare.core.anno.lifecycle.Step3;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * AnnoManager 注解管理器单元测试
 *
 * @author Flare Team
 * @since 1.0.0
 */
public class AnnoManagerTest {

    /**
     * 测试类：包含 @Process 注解的方法
     */
    static class TestProcessClass extends BaseFlare {
        private boolean processCalled = false;
        private boolean process2Called = false;

        @Override
        public JobType getJobType() {
            return null;
        }

        @Override
        protected String resourceId() {
            return "test-resource-id";
        }

        @Override
        protected void createContext(Object conf) {
            // 测试用，不需要实际创建上下文
        }

        @Override
        public void process() {
            // 测试用
        }

        @Override
        public void stop() {
            // 测试用
        }

        @Override
        public void counter(String name, long count) {
            // 测试用
        }

        @Process("测试方法1")
        public void testProcess1() {
            processCalled = true;
        }

        @Process("测试方法2")
        public void testProcess2() {
            process2Called = true;
        }

        public boolean isProcessCalled() {
            return processCalled;
        }

        public boolean isProcess2Called() {
            return process2Called;
        }
    }

    /**
     * 测试类：包含 @Step 注解的方法
     */
    static class TestStepClass extends BaseFlare {
        private boolean step1Called = false;
        private boolean step2Called = false;
        private boolean step3Called = false;
        private int callOrder = 0;
        private int step1Order = 0;
        private int step2Order = 0;
        private int step3Order = 0;

        @Override
        public JobType getJobType() {
            return null;
        }

        @Override
        protected String resourceId() {
            return "test-resource-id";
        }

        @Override
        protected void createContext(Object conf) {
            // 测试用
        }

        @Override
        public void process() {
            // 测试用
        }

        @Override
        public void stop() {
            // 测试用
        }

        @Override
        public void counter(String name, long count) {
            // 测试用
        }

        @Step1("步骤1")
        public void testStep1() {
            step1Called = true;
            step1Order = ++callOrder;
        }

        @Step2("步骤2")
        public void testStep2() {
            step2Called = true;
            step2Order = ++callOrder;
        }

        @Step3("步骤3")
        public void testStep3() {
            step3Called = true;
            step3Order = ++callOrder;
        }

        public boolean isStep1Called() {
            return step1Called;
        }

        public boolean isStep2Called() {
            return step2Called;
        }

        public boolean isStep3Called() {
            return step3Called;
        }

        public int getStep1Order() {
            return step1Order;
        }

        public int getStep2Order() {
            return step2Order;
        }

        public int getStep3Order() {
            return step3Order;
        }
    }

    /**
     * 测试类：包含 @Before 和 @After 注解的方法
     */
    static class TestLifecycleClass extends BaseFlare {
        private boolean beforeCalled = false;
        private boolean afterCalled = false;

        @Override
        public JobType getJobType() {
            return null;
        }

        @Override
        protected String resourceId() {
            return "test-resource-id";
        }

        @Override
        protected void createContext(Object conf) {
            // 测试用
        }

        @Override
        public void process() {
            // 测试用
        }

        @Override
        public void stop() {
            // 测试用
        }

        @Override
        public void counter(String name, long count) {
            // 测试用
        }

        @Before
        public void testBefore() {
            beforeCalled = true;
        }

        @After
        public void testAfter() {
            afterCalled = true;
        }

        public boolean isBeforeCalled() {
            return beforeCalled;
        }

        public boolean isAfterCalled() {
            return afterCalled;
        }
    }

    @Test
    public void testProcessAnno() {
        // 测试 @Process 注解方法调用
        TestProcessClass testObj = new TestProcessClass();
        
        assertFalse(testObj.isProcessCalled());
        assertFalse(testObj.isProcess2Called());
        
        AnnoManager.processAnno(testObj);
        
        assertTrue(testObj.isProcessCalled());
        assertTrue(testObj.isProcess2Called());
    }

    @Test
    public void testStepAnno() {
        // 测试 @Step 注解方法按顺序调用
        TestStepClass testObj = new TestStepClass();
        
        assertFalse(testObj.isStep1Called());
        assertFalse(testObj.isStep2Called());
        assertFalse(testObj.isStep3Called());
        
        AnnoManager.processAnno(testObj);
        
        // 验证所有 Step 方法都被调用
        assertTrue(testObj.isStep1Called());
        assertTrue(testObj.isStep2Called());
        assertTrue(testObj.isStep3Called());
        
        // 验证调用顺序：Step1 -> Step2 -> Step3
        assertTrue(testObj.getStep1Order() < testObj.getStep2Order());
        assertTrue(testObj.getStep2Order() < testObj.getStep3Order());
    }

    @Test
    public void testLifeCycleAnno() {
        // 测试生命周期注解方法调用
        TestLifecycleClass testObj = new TestLifecycleClass();
        
        assertFalse(testObj.isBeforeCalled());
        assertFalse(testObj.isAfterCalled());
        
        AnnoManager.lifeCycleAnno(testObj, Before.class);
        assertTrue(testObj.isBeforeCalled());
        assertFalse(testObj.isAfterCalled());
        
        AnnoManager.lifeCycleAnno(testObj, After.class);
        assertTrue(testObj.isAfterCalled());
    }

    @Test
    public void testNullBaseFlare() {
        // 测试 null 参数处理
        try {
            AnnoManager.processAnno(null);
            // 应该不会抛出异常，只是记录警告
        } catch (Exception e) {
            fail("处理 null 参数不应该抛出异常");
        }
    }
}

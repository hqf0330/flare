package com.bhcode.flare.examples;

import com.bhcode.flare.flink.FlareTestBase;
import com.bhcode.flare.flink.util.TestSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * 演示：如何使用 FlareTestBase 编写业务逻辑单元测试
 */
public class BusinessLogicTest extends FlareTestBase {

    @Test
    public void testLogic() throws Exception {
        // 1. 准备模拟数据
        List<String> input = Arrays.asList("apple", "banana", "cherry");
        
        // 2. 创建模拟源
        DataStream<String> source = createMockSource(input);
        
        // 3. 编写待测逻辑
        DataStream<String> result = source
                .filter(s -> s.startsWith("a") || s.startsWith("b"))
                .map(String::toUpperCase);
        
        // 4. 绑定测试 Sink
        TestSink.clear();
        result.addSink(new TestSink<>());
        
        // 5. 执行任务
        env.execute("TestBusinessLogic");
        
        // 6. 验证结果
        List<String> expected = Arrays.asList("APPLE", "BANANA");
        Assert.assertEquals(expected, TestSink.getResults());
    }
}

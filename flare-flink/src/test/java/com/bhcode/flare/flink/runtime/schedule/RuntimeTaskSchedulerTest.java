package com.bhcode.flare.flink.runtime.schedule;

import com.bhcode.flare.common.anno.Scheduled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

public class RuntimeTaskSchedulerTest {

    private RuntimeTaskScheduler scheduler;

    @After
    public void tearDown() {
        if (this.scheduler != null) {
            this.scheduler.close();
        }
    }

    @Test
    public void shouldExecuteScheduledMethodWithFixedInterval() throws Exception {
        TickTask task = new TickTask();
        this.scheduler = new RuntimeTaskScheduler(1, "test-scheduler-");

        int registered = this.scheduler.register(task);

        Assert.assertEquals(1, registered);
        await(() -> task.counter.get() >= 3, 1500);
        Assert.assertTrue(task.counter.get() >= 3);
    }

    @Test
    public void shouldHonorRepeatCount() throws Exception {
        RepeatTask task = new RepeatTask();
        this.scheduler = new RuntimeTaskScheduler(1, "test-scheduler-");

        int registered = this.scheduler.register(task);

        Assert.assertEquals(1, registered);
        Thread.sleep(400);
        Assert.assertEquals(3, task.counter.get());
    }

    @Test
    public void shouldIgnoreScheduledMethodWithArguments() {
        InvalidTask task = new InvalidTask();
        this.scheduler = new RuntimeTaskScheduler(1, "test-scheduler-");

        int registered = this.scheduler.register(task);

        Assert.assertEquals(0, registered);
    }

    private static void await(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        Assert.fail("Condition was not met within " + timeoutMs + " ms");
    }

    static final class TickTask {
        private final AtomicInteger counter = new AtomicInteger();

        @Scheduled(fixedInterval = 50, initialDelay = 0)
        public void tick() {
            this.counter.incrementAndGet();
        }
    }

    static final class RepeatTask {
        private final AtomicInteger counter = new AtomicInteger();

        @Scheduled(fixedInterval = 30, initialDelay = 0, repeatCount = 3)
        public void tick() {
            this.counter.incrementAndGet();
        }
    }

    static final class InvalidTask {

        @Scheduled(fixedInterval = 10)
        public void invalid(String ignored) {
            // invalid scheduled signature
        }
    }
}

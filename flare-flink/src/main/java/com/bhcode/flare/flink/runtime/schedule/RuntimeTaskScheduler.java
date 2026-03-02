package com.bhcode.flare.flink.runtime.schedule;

import com.bhcode.flare.common.anno.Scheduled;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class RuntimeTaskScheduler implements AutoCloseable {

    private final ScheduledExecutorService executor;
    private final List<ScheduledFuture<?>> futures = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public RuntimeTaskScheduler(int poolSize, String threadNamePrefix) {
        int finalPoolSize = Math.max(1, poolSize);
        String finalPrefix = (threadNamePrefix == null || threadNamePrefix.trim().isEmpty())
                ? "flare-schedule-"
                : threadNamePrefix.trim();
        this.executor = Executors.newScheduledThreadPool(
                finalPoolSize,
                runnable -> {
                    Thread t = new Thread(runnable);
                    t.setDaemon(true);
                    t.setName(finalPrefix + t.getId());
                    return t;
                }
        );
    }

    /**
     * 注册并启动定时任务。
     *
     * @param taskInstances 包含 @Scheduled 方法的实例
     * @return 成功注册的任务数量
     */
    public int register(Object... taskInstances) {
        ensureOpen();
        if (taskInstances == null || taskInstances.length == 0) {
            return 0;
        }
        int count = 0;
        for (Object task : taskInstances) {
            if (task == null) {
                continue;
            }
            for (Method method : findAnnotatedMethods(task.getClass())) {
                Scheduled scheduled = method.getAnnotation(Scheduled.class);
                if (scheduled == null || !scheduled.enabled()) {
                    continue;
                }
                if (!isValidSignature(method)) {
                    log.warn("Ignore invalid @Scheduled method (must be void and no-args): {}#{}",
                            task.getClass().getName(), method.getName());
                    continue;
                }
                if (scheduled.fixedInterval() <= 0) {
                    log.warn("Ignore @Scheduled method with non-positive interval: {}#{} interval={}",
                            task.getClass().getName(), method.getName(), scheduled.fixedInterval());
                    continue;
                }
                if (scheduled.repeatCount() == 0) {
                    continue;
                }
                method.setAccessible(true);
                scheduleTask(task, method, scheduled);
                count++;
            }
        }
        return count;
    }

    @Override
    public void close() {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }
        for (ScheduledFuture<?> future : this.futures) {
            if (future != null) {
                future.cancel(false);
            }
        }
        this.futures.clear();
        this.executor.shutdownNow();
    }

    private void scheduleTask(Object task, Method method, Scheduled scheduled) {
        long interval = scheduled.fixedInterval();
        long initialDelay = Math.max(0L, scheduled.initialDelay());
        long repeatCount = scheduled.repeatCount();

        AtomicLong executedCount = new AtomicLong(0L);
        ScheduledFutureHolder holder = new ScheduledFutureHolder();
        Runnable command = () -> execute(task, method, repeatCount, executedCount, holder);

        ScheduledFuture<?> future = this.executor.scheduleWithFixedDelay(
                command,
                initialDelay,
                interval,
                TimeUnit.MILLISECONDS
        );
        holder.future = future;
        this.futures.add(future);
        log.info("Registered runtime scheduled task: {}#{} interval={}ms initialDelay={}ms repeatCount={}",
                task.getClass().getName(),
                method.getName(),
                interval,
                initialDelay,
                repeatCount);
    }

    private void execute(
            Object task,
            Method method,
            long repeatCount,
            AtomicLong executedCount,
            ScheduledFutureHolder holder) {
        long current = executedCount.incrementAndGet();
        if (repeatCount > 0 && current > repeatCount) {
            cancelFuture(holder.future);
            return;
        }
        try {
            method.invoke(task);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            log.error("Runtime scheduled task failed: {}#{}", task.getClass().getName(), method.getName(), cause);
        } catch (Exception e) {
            log.error("Runtime scheduled task failed: {}#{}", task.getClass().getName(), method.getName(), e);
        } finally {
            if (repeatCount > 0 && current >= repeatCount) {
                cancelFuture(holder.future);
            }
        }
    }

    private void cancelFuture(ScheduledFuture<?> future) {
        if (future != null) {
            future.cancel(false);
            this.futures.remove(future);
        }
    }

    private void ensureOpen() {
        if (this.closed.get()) {
            throw new IllegalStateException("RuntimeTaskScheduler is already closed");
        }
    }

    private boolean isValidSignature(Method method) {
        return method.getParameterCount() == 0
                && Void.TYPE.equals(method.getReturnType());
    }

    private List<Method> findAnnotatedMethods(Class<?> clazz) {
        List<Method> methods = new ArrayList<>();
        Set<String> seen = new java.util.HashSet<>();
        Class<?> cursor = clazz;
        while (cursor != null && !Object.class.equals(cursor)) {
            for (Method method : cursor.getDeclaredMethods()) {
                if (!method.isAnnotationPresent(Scheduled.class)) {
                    continue;
                }
                String key = signatureKey(method);
                if (seen.add(key)) {
                    methods.add(method);
                }
            }
            cursor = cursor.getSuperclass();
        }
        return methods;
    }

    private String signatureKey(Method method) {
        StringBuilder sb = new StringBuilder(method.getName())
                .append("#")
                .append(method.getReturnType().getName());
        Class<?>[] parameterTypes = method.getParameterTypes();
        for (Class<?> parameterType : parameterTypes) {
            sb.append("#").append(parameterType.getName());
        }
        return sb.toString();
    }

    private static final class ScheduledFutureHolder {
        private ScheduledFuture<?> future;
    }
}

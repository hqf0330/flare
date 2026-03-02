package com.bhcode.flare.common.util;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class ExceptionBus {
    private static final Queue<Throwable> exceptions = new ConcurrentLinkedQueue<>();

    public static void post(Throwable t) {
        if (t != null) {
            exceptions.offer(t);
            // 这里以后可以扩展发送到 MQ 或 飞书/钉钉
            log.error("Exception posted to ExceptionBus: {}", t.getMessage());
        }
    }

    public static void offAndLogError(org.slf4j.Logger logger, String msg, Throwable t) {
        post(t);
        if (logger != null) {
            logger.error(msg, t);
        } else {
            log.error(msg, t);
        }
    }

    /**
     * 按顺序读取队列中的异常快照，不清空队列。
     */
    public static List<Throwable> snapshot(int limit) {
        int max = normalizeLimit(limit);
        List<Throwable> list = new ArrayList<>(Math.min(max, 128));
        int count = 0;
        for (Throwable throwable : exceptions) {
            if (throwable == null) {
                continue;
            }
            list.add(throwable);
            count++;
            if (count >= max) {
                break;
            }
        }
        return list;
    }

    /**
     * 读取并清空（最多 limit 条）异常。
     */
    public static List<Throwable> drain(int limit) {
        int max = normalizeLimit(limit);
        List<Throwable> list = new ArrayList<>(Math.min(max, 128));
        for (int i = 0; i < max; i++) {
            Throwable throwable = exceptions.poll();
            if (throwable == null) {
                break;
            }
            list.add(throwable);
        }
        return list;
    }

    public static int size() {
        return exceptions.size();
    }

    private static int normalizeLimit(int limit) {
        if (limit <= 0) {
            return 100;
        }
        return Math.min(limit, 1000);
    }
}

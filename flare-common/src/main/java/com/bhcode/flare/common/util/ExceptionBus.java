package com.bhcode.flare.common.util;

import lombok.extern.slf4j.Slf4j;
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
}

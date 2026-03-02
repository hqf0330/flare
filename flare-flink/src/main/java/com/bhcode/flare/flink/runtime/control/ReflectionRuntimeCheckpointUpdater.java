package com.bhcode.flare.flink.runtime.control;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

@Slf4j
public class ReflectionRuntimeCheckpointUpdater implements RuntimeCheckpointUpdater {

    @Override
    public boolean apply(CheckpointControlRequest request) {
        if (request == null) {
            return false;
        }
        try {
            Class<?> coordinatorClass = Class.forName("org.apache.flink.runtime.checkpoint.CheckpointCoordinator");
            Method getInstance = findMethod(coordinatorClass, "getInstance");
            if (getInstance == null || !Modifier.isStatic(getInstance.getModifiers())) {
                return false;
            }
            getInstance.setAccessible(true);
            Object coordinator = getInstance.invoke(null);
            if (coordinator == null) {
                return false;
            }

            boolean touched = false;
            touched |= invokeLongSetter(coordinatorClass, coordinator, "setBaseInterval", request.interval());
            touched |= invokeLongSetter(coordinatorClass, coordinator, "setCheckpointTimeout", request.timeout());
            touched |= invokeLongSetter(
                    coordinatorClass, coordinator, "setMinPauseBetweenCheckpoints", request.minPauseBetween());

            if (touched) {
                Method startScheduler = findMethod(coordinatorClass, "startCheckpointScheduler");
                if (startScheduler != null) {
                    startScheduler.setAccessible(true);
                    startScheduler.invoke(coordinator);
                }
            }
            return touched;
        } catch (Exception e) {
            log.debug("Runtime checkpoint hot update is not available in current runtime", e);
            return false;
        }
    }

    private boolean invokeLongSetter(Class<?> clazz, Object target, String methodName, Long value) {
        if (value == null || value <= 0) {
            return false;
        }
        try {
            Method method = findMethod(clazz, methodName, long.class);
            if (method == null) {
                return false;
            }
            method.setAccessible(true);
            method.invoke(target, value.longValue());
            return true;
        } catch (Exception e) {
            log.debug("Failed to invoke checkpoint setter: {}", methodName, e);
            return false;
        }
    }

    private Method findMethod(Class<?> clazz, String name, Class<?>... types) {
        try {
            return clazz.getMethod(name, types);
        } catch (NoSuchMethodException ignore) {
            try {
                return clazz.getDeclaredMethod(name, types);
            } catch (NoSuchMethodException ignoredAgain) {
                return null;
            }
        }
    }
}

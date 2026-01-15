package com.bhcode.flare.core;

import com.bhcode.flare.core.anno.connector.Process;
import com.bhcode.flare.core.anno.lifecycle.Step1;
import com.bhcode.flare.core.anno.lifecycle.Step2;
import com.bhcode.flare.core.anno.lifecycle.Step3;
import com.bhcode.flare.core.anno.lifecycle.Step4;
import com.bhcode.flare.core.anno.lifecycle.Step5;
import com.bhcode.flare.core.anno.lifecycle.Step6;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class AnnoManager {

    // 用于注册所有的生命周期注解（@Process 注解）
    private static final List<Class<? extends Annotation>> REGISTER_ANNO_METHODS = Arrays.asList(
            Process.class
    );

    // Step 注解列表（按顺序）
    private static final List<Class<? extends Annotation>> STEP_ANNO_METHODS = Arrays.asList(
            Step1.class, Step2.class, Step3.class, Step4.class, Step5.class, Step6.class
    );

    /**
     * 用于调用生命周期注解所标记的方法
     * <p>
     * 执行顺序：
     * 1. 先执行 @Process 注解的方法（按方法名排序）
     * 2. 再执行 @Step1, @Step2, @Step3... 注解的方法（按 Step 顺序）
     * </p>
     *
     * @param baseFlare BaseFlare 实例
     */
    public static void processAnno(BaseFlare baseFlare) {
        if (baseFlare == null) {
            log.warn("BaseFlare instance is null, skip annotation processing");
            return;
        }

        // 1. 先执行 @Process 注解的方法
        invokeAnnoMethods(baseFlare, REGISTER_ANNO_METHODS);
        
        // 2. 再执行 @Step 系列注解的方法（按 Step1 -> Step6 顺序）
        invokeAnnoMethods(baseFlare, STEP_ANNO_METHODS);
    }

    /**
     * 用于调用指定的被注解标记的生命周期方法
     *
     * @param baseFlare BaseFlare 实例
     * @param annoClass 注解类型
     */
    public static void lifeCycleAnno(BaseFlare baseFlare, Class<? extends Annotation> annoClass) {
        if (baseFlare == null || annoClass == null) {
            log.warn("BaseFlare instance or annotation class is null, skip lifecycle annotation processing");
            return;
        }
        invokeAnnoMethods(baseFlare, Arrays.asList(annoClass));
    }

    /**
     * 调用被指定注解标记的方法
     *
     * @param baseFlare      BaseFlare 实例
     * @param annotationClasses 注解类型列表
     */
    private static void invokeAnnoMethods(BaseFlare baseFlare, List<Class<? extends Annotation>> annotationClasses) {
        Class<?> clazz = baseFlare.getClass();
        Method[] methods = clazz.getDeclaredMethods();

        // 收集并排序待执行的方法
        List<MethodWithAnno> methodsToInvoke = new ArrayList<>();

        for (Class<? extends Annotation> annoClass : annotationClasses) {
            for (Method method : methods) {
                if (method.isAnnotationPresent(annoClass)) {
                    if (method.getParameterCount() == 0) {
                        methodsToInvoke.add(new MethodWithAnno(method, method.getAnnotation(annoClass)));
                    } else {
                        log.warn("Method {} has parameters, skip invocation", method.getName());
                    }
                }
            }
        }

        // 同一优先级的按方法名排序
        methodsToInvoke.sort(Comparator.comparing(m -> m.method.getName()));

        // 执行调用
        for (MethodWithAnno entry : methodsToInvoke) {
            Method method = entry.method;
            Annotation annotation = entry.annotation;
            String name = method.getName();
            
            try {
                method.setAccessible(true);
                
                // 解析描述和错误跳过策略
                String description = "";
                boolean skipError = false;
                
                if (annotation instanceof Process p) {
                    description = p.value();
                    skipError = p.skipError();
                } else if (annotation != null) {
                    // 使用反射获取 Step 系列注解的通用属性（仅在此处反射一次）
                    description = getAnnotationValue(annotation, "value", "");
                    skipError = getAnnotationValue(annotation, "skipError", false);
                }

                if (!description.isEmpty()) {
                    log.info("Invoking method: {} ({})", name, description);
                } else {
                    log.info("Invoking method: {}", name);
                }

                long startTime = System.currentTimeMillis();
                method.invoke(baseFlare);
                log.debug("Method {} executed in {} ms", name, System.currentTimeMillis() - startTime);
                
            } catch (Exception e) {
                boolean skipErrorFlag = false;
                if (annotation instanceof Process p) skipErrorFlag = p.skipError();
                else skipErrorFlag = getAnnotationValue(annotation, "skipError", false);

                if (skipErrorFlag) {
                    log.warn("Method {} failed but skipError=true, continuing...", name, e);
                } else {
                    log.error("Method {} failed", name, e);
                    throw new RuntimeException("Execution failed for method: " + name, e);
                }
            }
        }
    }

    private static <T> T getAnnotationValue(Annotation anno, String methodName, T defaultValue) {
        try {
            Method method = anno.annotationType().getMethod(methodName);
            return (T) method.invoke(anno);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static record MethodWithAnno(Method method, Annotation annotation) {}
}

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

        try {
            // 1. 先执行 @Process 注解的方法
            invokeAnnoMethods(baseFlare, REGISTER_ANNO_METHODS);
            
            // 2. 再执行 @Step 注解的方法（按顺序）
            invokeStepAnnoMethods(baseFlare);
        } catch (Exception e) {
            log.error("Failed to process annotation methods", e);
            throw new RuntimeException("Failed to process annotation methods", e);
        }
    }

    /**
     * 按顺序调用 @Step 注解的方法
     * <p>
     * 执行顺序：Step1 -> Step2 -> Step3 -> Step4 -> Step5 -> Step6
     * </p>
     *
     * @param baseFlare BaseFlare 实例
     */
    private static void invokeStepAnnoMethods(BaseFlare baseFlare) {
        Class<?> clazz = baseFlare.getClass();
        Method[] methods = clazz.getDeclaredMethods();

        // 按 Step 注解类型分组
        Map<Class<? extends Annotation>, List<Method>> stepMethodsMap = new HashMap<>();
        for (Class<? extends Annotation> stepAnno : STEP_ANNO_METHODS) {
            stepMethodsMap.put(stepAnno, new ArrayList<>());
        }

        // 收集所有带 Step 注解的方法
        for (Method method : methods) {
            for (Class<? extends Annotation> stepAnno : STEP_ANNO_METHODS) {
                if (method.isAnnotationPresent(stepAnno)) {
                    if (method.getParameterCount() == 0) {
                        stepMethodsMap.get(stepAnno).add(method);
                    } else {
                        log.warn("Step method {} has parameters, skip invocation", method.getName());
                    }
                    break; // 一个方法只能有一个 Step 注解
                }
            }
        }

        // 按 Step 顺序执行
        for (Class<? extends Annotation> stepAnno : STEP_ANNO_METHODS) {
            List<Method> methodsToInvoke = stepMethodsMap.get(stepAnno);
            if (methodsToInvoke.isEmpty()) {
                continue;
            }

            // 同一 Step 内的多个方法按方法名排序
            methodsToInvoke.sort(Comparator.comparing(Method::getName));

            for (Method method : methodsToInvoke) {
                Annotation annotation = null;
                try {
                    method.setAccessible(true);
                    
                    // 获取 Step 注解的描述信息
                    annotation = method.getAnnotation(stepAnno);
                    String stepName = stepAnno.getSimpleName();
                    String description = getStepDescription(annotation, stepAnno);
                    
                    if (description != null && !description.isEmpty()) {
                        log.info("Step {}. {} - {}", stepName, description, method.getName());
                    } else {
                        log.info("Step {}. {}", stepName, method.getName());
                    }

                    long startTime = System.currentTimeMillis();
                    method.invoke(baseFlare);
                    long elapsed = System.currentTimeMillis() - startTime;
                    
                    log.info("Step {} completed. Elapsed: {} ms", stepName, elapsed);
                } catch (Exception e) {
                    // 检查是否应该跳过错误
                    boolean skipError = annotation != null ? getStepSkipError(annotation, stepAnno) : false;
                    
                    if (skipError) {
                        log.warn("Step {} method {} failed but skipError=true, continue", 
                                stepAnno.getSimpleName(), method.getName(), e);
                    } else {
                        log.error("Step {} method {} failed", stepAnno.getSimpleName(), method.getName(), e);
                        throw new RuntimeException("Step " + stepAnno.getSimpleName() + 
                                " method " + method.getName() + " failed", e);
                    }
                }
            }
        }
    }

    /**
     * 获取 Step 注解的描述信息
     */
    private static String getStepDescription(Annotation annotation, Class<? extends Annotation> stepAnno) {
        try {
            Method valueMethod = stepAnno.getMethod("value");
            return (String) valueMethod.invoke(annotation);
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 获取 Step 注解的 skipError 属性
     */
    private static boolean getStepSkipError(Annotation annotation, Class<? extends Annotation> stepAnno) {
        try {
            Method skipErrorMethod = stepAnno.getMethod("skipError");
            return (Boolean) skipErrorMethod.invoke(annotation);
        } catch (Exception e) {
            return false;
        }
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

        try {
            invokeAnnoMethods(baseFlare, Arrays.asList(annoClass));
        } catch (Exception e) {
            log.error("Failed to process lifecycle annotation: {}", annoClass.getName(), e);
            throw new RuntimeException("Failed to process lifecycle annotation: " + annoClass.getName(), e);
        }
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

        // 按注解类型分组，并按方法名排序（保证执行顺序）
        List<Method> methodsToInvoke = new ArrayList<>();

        for (Class<? extends Annotation> annoClass : annotationClasses) {
            for (Method method : methods) {
                if (method.isAnnotationPresent(annoClass)) {
                    // 检查方法签名：应该是无参或只有一个参数的方法
                    if (method.getParameterCount() == 0) {
                        methodsToInvoke.add(method);
                    } else {
                        log.warn("Method {} has parameters, skip invocation", method.getName());
                    }
                }
            }
        }

        // 按方法名排序，保证执行顺序
        methodsToInvoke.sort((m1, m2) -> m1.getName().compareTo(m2.getName()));

        // 调用方法
        for (Method method : methodsToInvoke) {
            try {
                method.setAccessible(true);
                Annotation annotation = method.getAnnotation(Process.class);
                if (annotation != null) {
                    Process processAnno = (Process) annotation;
                    String description = processAnno.value();
                    if (description != null && !description.isEmpty()) {
                        log.debug("Invoking @Process method: {} - {}", method.getName(), description);
                    } else {
                        log.debug("Invoking @Process method: {}", method.getName());
                    }
                } else {
                    log.debug("Invoking annotated method: {}", method.getName());
                }

                method.invoke(baseFlare);
                log.debug("Successfully invoked method: {}", method.getName());
            } catch (Exception e) {
                // 检查是否应该跳过错误
                Process processAnno = method.getAnnotation(Process.class);
                boolean skipError = processAnno != null && processAnno.skipError();

                if (skipError) {
                    log.warn("Failed to invoke method {} but skipError=true, continue", method.getName(), e);
                } else {
                    log.error("Failed to invoke method: {}", method.getName(), e);
                    throw new RuntimeException("Failed to invoke method: " + method.getName(), e);
                }
            }
        }
    }
}

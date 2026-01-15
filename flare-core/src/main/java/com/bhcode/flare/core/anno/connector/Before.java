package com.bhcode.flare.core.anno.connector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @description: 用于标注生命周期方法，在引擎初始化前被调用执行，可用于资源初始化
 * @author binghu
 * @date 2026/1/14 23:28
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Before {
}

package com.bhcode.flare.common.bean.analysis;

import com.bhcode.flare.common.bean.FlareTask;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author binghu
 * @description: 异常堆栈信息
 * @date 2026/1/14 23:20
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExceptionMsg extends FlareTask {

    /**
     * 异常堆栈类名
     */
    private String exceptionClass;

    /**
     * 异常堆栈的标题
     */
    private String stackTitle;

    /**
     * 异常堆栈详细信息
     */
    private String stackTrace;

    /**
     * 发送异常的sql语句
     */
    private String sql;
}

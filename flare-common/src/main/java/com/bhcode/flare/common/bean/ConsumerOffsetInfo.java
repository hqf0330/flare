package com.bhcode.flare.common.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author binghu
 * @description: MQ消费信息
 * @date 2026/1/14 22:35
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerOffsetInfo {

    /**
     * topic主题
     */
    private String topic;

    /**
     * broker服务
     */
    private String broker;

    /**
     * 分区id
     */
    private Integer partition;

    /**
     * 消费位移
     */
    private Long offset;

    /**
     * 消费时间
     */
    private Long timestamp;
}

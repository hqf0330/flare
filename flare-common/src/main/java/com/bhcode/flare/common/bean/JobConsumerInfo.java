package com.bhcode.flare.common.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

/**
 * @description: 任务消费者信息
 * @author binghu
 * @date 2026/1/14 22:53
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobConsumerInfo extends FlareTask {

    private Set<ConsumerOffsetInfo> offsets;

}

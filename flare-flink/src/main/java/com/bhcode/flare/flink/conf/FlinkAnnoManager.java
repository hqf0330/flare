package com.bhcode.flare.flink.conf;

import com.bhcode.flare.common.util.PropUtils;
import com.bhcode.flare.flink.anno.Checkpoint;
import com.bhcode.flare.flink.anno.State;
import com.bhcode.flare.flink.anno.Streaming;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class FlinkAnnoManager {

    private final Map<String, Object> props = new HashMap<>();

    /**
     * 将时间单位由秒转为毫秒
     *
     * @param value 秒数
     * @return 毫秒数，如果 value <= 0 则返回 -1
     */
    private int unitConversion(int value) {
        return value > 0 ? value * 1000 : -1;
    }

    /**
     * 将 @Streaming 中配置的信息映射为键值对形式
     *
     * @param streaming Streaming 注解实例
     */
    public void mapStreaming(Streaming streaming) {
        if (streaming == null) {
            return;
        }

        // checkpoint 相关配置
        int interval = streaming.value() > 0 ? streaming.value() : streaming.interval();
        if (interval > 0) {
            this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_INTERVAL, unitConversion(interval));
        }
        if (streaming.timeout() > 0) {
            this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_TIMEOUT, unitConversion(streaming.timeout()));
        }
        if (streaming.pauseBetween() > 0) {
            this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN, unitConversion(streaming.pauseBetween()));
        }
        this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_UNALIGNED, streaming.unaligned());
        if (streaming.concurrent() > 0) {
            this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT, streaming.concurrent());
        }
        if (streaming.failureNumber() >= 0) {
            this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER, streaming.failureNumber());
        }
        this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_MODE, streaming.mode().name());
        this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_EXTERNALIZED, streaming.cleanup().name());

        // 其他配置
        this.put(FlareFlinkConf.FLINK_JOB_AUTO_START, streaming.autoStart());
        if (streaming.parallelism() > 0) {
            this.put(FlareFlinkConf.FLINK_DEFAULT_PARALLELISM, streaming.parallelism());
        }
        this.put(FlareFlinkConf.OPERATOR_CHAINING_ENABLE, !streaming.disableOperatorChaining());
        this.put(FlareFlinkConf.FLINK_STATE_TTL_DAYS, streaming.stateTTL());
        this.put(FlareFlinkConf.FLINK_SQL_USE_STATEMENT_SET, streaming.useStatementSet());
        this.put(FlareFlinkConf.FLINK_RUNTIME_MODE, streaming.executionMode().name());
        if (streaming.watermarkInterval() > 0) {
            this.put(FlareFlinkConf.FLINK_AUTO_WATERMARK_INTERVAL, streaming.watermarkInterval());
        }
    }

    /**
     * 将 @Checkpoint 中配置的信息映射为键值对形式
     *
     * @param checkpoint Checkpoint 注解实例
     */
    public void mapCheckpoint(Checkpoint checkpoint) {
        if (checkpoint == null) {
            return;
        }

        // checkpoint 相关配置
        int interval = checkpoint.value() > 0 ? checkpoint.value() : checkpoint.interval();
        if (interval > 0) {
            this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_INTERVAL, unitConversion(interval));
        }
        if (checkpoint.timeout() > 0) {
            this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_TIMEOUT, unitConversion(checkpoint.timeout()));
        }
        if (checkpoint.pauseBetween() > 0) {
            this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN, unitConversion(checkpoint.pauseBetween()));
        }
        this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_UNALIGNED, checkpoint.unaligned());
        if (checkpoint.concurrent() > 0) {
            this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT, checkpoint.concurrent());
        }
        if (checkpoint.failureNumber() >= 0) {
            this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER, checkpoint.failureNumber());
        }
        this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_MODE, checkpoint.mode().name());
        this.put(FlareFlinkConf.FLINK_STREAM_CHECKPOINT_EXTERNALIZED, checkpoint.cleanup().name());
    }

    /**
     * 将 @State 中配置的信息映射为键值对形式
     *
     * @param state State 注解实例
     */
    public void mapState(State state) {
        if (state == null) {
            return;
        }

        if (state.backend() != null && !state.backend().isEmpty()) {
            this.put("state.backend", state.backend().toLowerCase());
        }
        if (state.checkpointDir() != null && !state.checkpointDir().isEmpty()) {
            this.put("state.checkpoints.dir", state.checkpointDir());
        }
        if (state.savepointDir() != null && !state.savepointDir().isEmpty()) {
            this.put("state.savepoints.dir", state.savepointDir());
        }
        this.put("state.backend.incremental", state.incremental());
        if (state.ttl() > 0) {
            this.put(FlareFlinkConf.FLINK_STATE_TTL_DAYS, state.ttl());
        }
    }

    /**
     * 添加配置项
     *
     * @param key   配置键
     * @param value 配置值
     */
    public void put(String key, Object value) {
        if (key != null && value != null) {
            this.props.put(key, value);
        }
    }

    /**
     * 获取配置项
     *
     * @param key 配置键
     * @return 配置值
     */
    public Object get(String key) {
        return this.props.get(key);
    }

    /**
     * 获取所有配置项
     *
     * @return 配置项 Map
     */
    public Map<String, Object> getProps() {
        return new HashMap<>(this.props);
    }

    /**
     * 将当前配置项写入 PropUtils
     */
    public void applyToPropUtils() {
        this.props.forEach((key, value) -> {
            if (key != null && value != null) {
                PropUtils.setProperty(key, String.valueOf(value));
            }
        });
    }

    /**
     * 清空所有配置项
     */
    public void clear() {
        this.props.clear();
    }
}

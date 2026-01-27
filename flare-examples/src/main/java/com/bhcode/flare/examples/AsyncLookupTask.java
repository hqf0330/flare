package com.bhcode.flare.examples;

import com.bhcode.flare.core.anno.connector.AsyncLookup;
import com.bhcode.flare.core.anno.connector.Jdbc;
import com.bhcode.flare.core.anno.connector.Kafka;
import com.bhcode.flare.core.anno.connector.Redis;
import com.bhcode.flare.flink.FlinkJobLauncher;
import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.functions.FlareAsyncJdbcLookupFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import redis.clients.jedis.Jedis;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 演示：异步 I/O 维表关联
 */
@Slf4j
@Streaming(parallelism = 1)
@Kafka(
    brokers = "localhost:9092",
    topics = "user_actions",
    groupId = "async_lookup_group"
)
@Jdbc(
    keyNum = 2, // 维表通常是另一个库或配置
    url = "jdbc:mysql://localhost:3306/user_db",
    username = "root",
    password = "password"
)
@Redis(host = "localhost", port = 6379)
@AsyncLookup(cacheExpire = 60, cacheSize = 1000)
public class AsyncLookupTask extends FlinkStreaming {

    // 业务数据模型
    public record Action(String userId, String action) {}
    // 关联后的结果模型
    public record ActionWithUser(String userId, String userName, String level, String action) {}

    @Override
    public void process() {
        // 1. 读取 Kafka
        DataStream<Action> source = this.kafkaSourceFromConf(Action.class);

        // 2. 异步关联 MySQL 维表（方式一：Lambda 风格，最推荐，对标同步查询体验）
        DataStream<ActionWithUser> joinedStream = this.asyncJdbcLookup(
                source,
                2, // 显式指定 keyNum = 2，匹配类头部的 @Jdbc(keyNum = 2) 配置
                "SELECT user_name as userName, level FROM t_user WHERE user_id = ?",
                UserInfo.class,
                (ps, action) -> ps.setString(1, action.userId()),
                (user, action) -> {
                    if (user != null) {
                        return new ActionWithUser(action.userId(), user.userName(), user.level(), action.action());
                    }
                    return new ActionWithUser(action.userId(), "unknown", "0", action.action());
                }
        );

        // 3. 异步关联 Redis 维表（新增功能：B 计划）
        DataStream<ActionWithUser> redisJoinedStream = this.asyncRedisLookup(joinedStream, (jedis, input) -> {
            String city = jedis.get("city_" + input.userId());
            return new ActionWithUser(input.userId(), input.userName(), input.level(), input.action() + "_" + (city != null ? city : "unknown"));
        });

        // 4. 打印结果
        redisJoinedStream.print("final-data");
    }

    // 业务数据模型
    public record UserInfo(String userName, String level) {}

    /**
     * 自定义异步查询函数
     */
    @AsyncLookup(cacheSize = 5000, cacheExpire = 10) // 开启 10 秒 LRU 缓存
    public static class UserLookupFunction extends FlareAsyncJdbcLookupFunction<Action, ActionWithUser> {
        
        public UserLookupFunction(int keyNum) {
            super(keyNum);
        }

        @Override
        protected void fillParams(PreparedStatement ps, Action input) throws Exception {
            ps.setString(1, input.userId());
        }

        @Override
        protected ActionWithUser mapRow(ResultSet rs, Action input) throws Exception {
            return new ActionWithUser(
                input.userId(),
                rs.getString("user_name"),
                rs.getString("level"),
                input.action()
            );
        }
    }

    public static void main(String[] args) {
        FlinkJobLauncher.run(AsyncLookupTask.class, args);
    }
}

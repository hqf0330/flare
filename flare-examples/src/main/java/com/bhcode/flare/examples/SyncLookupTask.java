package com.bhcode.flare.examples;

import com.bhcode.flare.core.anno.connector.Jdbc;
import com.bhcode.flare.core.anno.connector.Kafka;
import com.bhcode.flare.flink.FlinkJobLauncher;
import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.functions.FlareRichMapFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Arrays;

/**
 * 演示：同步维表查询 (Sync Lookup)
 */
@Slf4j
@Streaming(parallelism = 1)
@Kafka(
    brokers = "localhost:9092",
    topics = "user_actions_sync",
    groupId = "sync_lookup_group"
)
@Jdbc(
    keyNum = 1,
    url = "jdbc:mysql://localhost:3306/user_db",
    username = "root",
    password = "password"
)
public class SyncLookupTask extends FlinkStreaming {

    public record Action(String userId, String action) {}
    public record UserInfo(String userId, String userName, String level) {}
    public record ActionWithUser(String userId, String userName, String level, String action) {}

    @Override
    public void process() {
        // 1. 模拟数据源
        DataStream<Action> source = this.getEnv().fromCollection(
                Arrays.asList(
                        new Action("1", "login"),
                        new Action("2", "buy")
                )
        );

        // 2. 同步关联维表
        DataStream<ActionWithUser> joinedStream = source.map(new FlareRichMapFunction<Action, ActionWithUser>() {
            @Override
            public ActionWithUser map(Action action) {
                // 像 fire 一样简洁：直接调用 queryOne，自动处理连接池和 Record 映射
                UserInfo user = this.queryOne(
                    "SELECT user_id as userId, user_name as userName, level FROM t_user WHERE user_id = ?", 
                    UserInfo.class, 
                    action.userId()
                );
                
                if (user != null) {
                    return new ActionWithUser(action.userId(), user.userName(), user.level(), action.action());
                }
                return new ActionWithUser(action.userId(), "unknown", "0", action.action());
            }
        });

        joinedStream.print("sync-joined-data");
    }

    public static void main(String[] args) {
        FlinkJobLauncher.run(SyncLookupTask.class, args);
    }
}

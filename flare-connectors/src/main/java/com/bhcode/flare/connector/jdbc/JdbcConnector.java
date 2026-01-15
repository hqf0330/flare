package com.bhcode.flare.connector.jdbc;

import com.bhcode.flare.common.lineage.LineageManager;
import com.bhcode.flare.common.util.PropUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.BiConsumer;

@Slf4j
public final class JdbcConnector {

    private JdbcConnector() {
        // Utility class
    }

    public static String jdbcPrefix(int keyNum) {
        validateKeyNum(keyNum);
        return keyNum == 1 ? "jdbc." : "jdbc" + keyNum + ".";
    }

    public static <T> void jdbcSinkFromConf(
            DataStream<T> stream,
            BiConsumer<PreparedStatement, T> binder,
            int keyNum
    ) {
        if (stream == null) {
            throw new IllegalArgumentException("stream is null");
        }
        if (binder == null) {
            throw new IllegalArgumentException("binder is null");
        }

        String prefix = jdbcPrefix(keyNum);
        String url = PropUtils.getString(prefix + "url");
        String user = PropUtils.getString(prefix + "user", "");
        String password = PropUtils.getString(prefix + "password", "");
        String driver = PropUtils.getString(prefix + "driver", "");
        String sql = PropUtils.getString(prefix + "sql");
        String upsertMode = PropUtils.getString(prefix + "upsert.mode", "none");
        String keyColumns = PropUtils.getString(prefix + "key.columns", "");

        if (url == null || url.trim().isEmpty()) {
            throw new IllegalStateException(prefix + "url is required");
        }
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalStateException(prefix + "sql is required");
        }

        // 处理 Upsert 逻辑
        if ("mysql".equalsIgnoreCase(upsertMode)) {
            sql = JdbcUpsertUtils.buildMysqlUpsertSql(sql, keyColumns);
            log.info("JDBC Upsert Mode [mysql] enabled. Final SQL: {}", sql);
        }

        int batchSize = PropUtils.getInt(prefix + "batch.size", 500);
        long batchInterval = PropUtils.getLong(prefix + "batch.interval.ms", 0);
        int maxRetries = PropUtils.getInt(prefix + "max.retries", 3);

        JdbcExecutionOptions execOptions = JdbcExecutionOptions.builder()
                .withBatchSize(batchSize)
                .withBatchIntervalMs(batchInterval)
                .withMaxRetries(maxRetries)
                .build();

        JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withUsername(user)
                .withPassword(password)
                .withDriverName(driver == null ? "" : driver)
                .build();

        stream.addSink(JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T t) throws SQLException {
                        binder.accept(ps, t);
                    }
                },
                execOptions,
                connOptions
        ));
        
        LineageManager.addLineage("Flink", "JDBC:" + url, "SINK");
    }

    private static void validateKeyNum(int keyNum) {
        if (keyNum <= 0) {
            throw new IllegalArgumentException("keyNum must be >= 1");
        }
    }
}

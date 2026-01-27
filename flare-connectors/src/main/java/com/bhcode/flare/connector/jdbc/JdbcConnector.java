package com.bhcode.flare.connector.jdbc;

import com.bhcode.flare.common.lineage.LineageManager;
import com.bhcode.flare.common.util.DBUtils;
import com.bhcode.flare.common.util.PropUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

@Slf4j
public final class JdbcConnector {

    private static final Map<Integer, HikariDataSource> dataSources = new ConcurrentHashMap<>();

    private JdbcConnector() {
        // Utility class
    }

    public static String jdbcPrefix(int keyNum) {
        validateKeyNum(keyNum);
        return keyNum == 1 ? "jdbc." : "jdbc" + keyNum + ".";
    }

    /**
     * Get or create a HikariDataSource for the given keyNum.
     */
    public static HikariDataSource getDataSource(int keyNum) {
        return dataSources.computeIfAbsent(keyNum, k -> {
            String prefix = jdbcPrefix(k);
            String url = PropUtils.getString(prefix + "url");
            String user = PropUtils.getString(prefix + "user", "");
            String password = PropUtils.getString(prefix + "password", "");
            String driver = PropUtils.getString(prefix + "driver", "");

            if (url == null || url.isEmpty()) {
                throw new IllegalStateException("JDBC URL is required for keyNum=" + k);
            }

            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(url);
            config.setUsername(user);
            config.setPassword(password);
            if (driver != null && !driver.isEmpty()) {
                config.setDriverClassName(driver);
            }

            // Default pool settings
            config.setMaximumPoolSize(PropUtils.getInt(prefix + "pool.max-size", 10));
            config.setMinimumIdle(PropUtils.getInt(prefix + "pool.min-idle", 2));
            config.setIdleTimeout(PropUtils.getLong(prefix + "pool.idle-timeout", 600000));
            config.setConnectionTimeout(PropUtils.getLong(prefix + "pool.connection-timeout", 30000));

            log.info("Creating HikariDataSource for keyNum={}, url={}", k, url);
            return new HikariDataSource(config);
        });
    }

    /**
     * Synchronous query for a list of objects.
     */
    public static <T> List<T> queryList(int keyNum, String sql, Object... params) {
        try (Connection conn = getDataSource(keyNum).getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }

            try (ResultSet rs = ps.executeQuery()) {
                // We need a way to pass the class type. 
                // For simplicity in this static helper, let's assume the caller will use the more specific one.
                throw new UnsupportedOperationException("Use queryList(keyNum, sql, clazz, params)");
            }
        } catch (SQLException e) {
            throw new RuntimeException("JDBC query failed", e);
        }
    }

    public static <T> List<T> queryList(int keyNum, String sql, Class<T> clazz, Object... params) {
        LineageManager.addLineage("JDBC:" + keyNum, "Flink", "SELECT");
        try (Connection conn = getDataSource(keyNum).getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }

            try (ResultSet rs = ps.executeQuery()) {
                return DBUtils.resultSetToBeanList(rs, clazz);
            }
        } catch (Exception e) {
            throw new RuntimeException("JDBC query failed", e);
        }
    }

    public static <T> T queryOne(int keyNum, String sql, Class<T> clazz, Object... params) {
        List<T> list = queryList(keyNum, sql, clazz, params);
        return list.isEmpty() ? null : list.get(0);
    }

    public static int executeUpdate(int keyNum, String sql, Object... params) {
        LineageManager.addLineage("Flink", "JDBC:" + keyNum, "UPDATE");
        try (Connection conn = getDataSource(keyNum).getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("JDBC update failed", e);
        }
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

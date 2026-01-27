package com.bhcode.flare.flink.functions;

import com.bhcode.flare.common.util.PropUtils;
import com.bhcode.flare.connector.jdbc.JdbcConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.CompletableFuture;

/**
 * JDBC implementation of asynchronous lookup.
 */
@Slf4j
public abstract class FlareAsyncJdbcLookupFunction<IN, OUT> extends FlareAsyncLookupFunction<IN, OUT> {

    protected final int keyNum;
    protected transient Connection connection;
    protected transient PreparedStatement preparedStatement;

    public FlareAsyncJdbcLookupFunction() {
        this(1);
    }

    public FlareAsyncJdbcLookupFunction(int keyNum) {
        this.keyNum = keyNum;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        String prefix = JdbcConnector.jdbcPrefix(keyNum);
        String url = PropUtils.getString(prefix + "url");
        String user = PropUtils.getString(prefix + "user", "");
        String password = PropUtils.getString(prefix + "password", "");
        String driver = PropUtils.getString(prefix + "driver", "");
        String sql = PropUtils.getString(prefix + "sql");

        if (url == null || url.isEmpty()) {
            throw new IllegalStateException("JDBC URL is required for async lookup, keyNum=" + keyNum);
        }

        if (driver != null && !driver.isEmpty()) {
            Class.forName(driver);
        }

        this.connection = DriverManager.getConnection(url, user, password);
        this.preparedStatement = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) preparedStatement.close();
        if (connection != null) connection.close();
    }

    @Override
    public CompletableFuture<OUT> lookup(IN input) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                synchronized (preparedStatement) {
                    fillParams(preparedStatement, input);
                    try (ResultSet rs = preparedStatement.executeQuery()) {
                        if (rs.next()) {
                            return mapRow(rs, input);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("JDBC async lookup error", e);
                throw new RuntimeException(e);
            }
            return null;
        }, executorService);
    }

    /**
     * Fill the prepared statement with parameters from the input.
     */
    protected abstract void fillParams(PreparedStatement ps, IN input) throws Exception;

    /**
     * Map the result set row to the output object.
     */
    protected abstract OUT mapRow(ResultSet rs, IN input) throws Exception;
}

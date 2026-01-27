package com.bhcode.flare.flink.functions;

import com.bhcode.flare.common.util.DBUtils;
import com.bhcode.flare.connector.jdbc.JdbcConnector;
import com.bhcode.flare.connector.jdbc.JdbcParameterBinder;
import com.bhcode.flare.connector.jdbc.JdbcResultJoiner;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Lambda-based JDBC asynchronous lookup function.
 */
@Slf4j
public class LambdaAsyncJdbcLookupFunction<IN, DIM, OUT> extends FlareAsyncLookupFunction<IN, OUT> {

    private final int keyNum;
    private final String sql;
    private final Class<DIM> dimClass;
    private final JdbcParameterBinder<IN> binder;
    private final JdbcResultJoiner<DIM, IN, OUT> joiner;

    private transient HikariDataSource dataSource;

    public LambdaAsyncJdbcLookupFunction(int keyNum, String sql, Class<DIM> dimClass, 
                                        JdbcParameterBinder<IN> binder, JdbcResultJoiner<DIM, IN, OUT> joiner) {
        this.keyNum = keyNum;
        this.sql = sql;
        this.dimClass = dimClass;
        this.binder = binder;
        this.joiner = joiner;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.dataSource = JdbcConnector.getDataSource(keyNum);
    }

    @Override
    public void close() throws Exception {
        super.close();
        // Note: HikariDataSource is managed by JdbcConnector singleton, 
        // we don't close it here to avoid affecting other tasks in the same JVM.
    }

    @Override
    public CompletableFuture<OUT> lookup(IN input) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                
                binder.accept(ps, input);
                try (ResultSet rs = ps.executeQuery()) {
                    List<DIM> dims = DBUtils.resultSetToBeanList(rs, dimClass);
                    DIM dim = dims.isEmpty() ? null : dims.get(0);
                    return joiner.join(dim, input);
                }
            } catch (Exception e) {
                log.error("Lambda JDBC async lookup error for sql: {}", sql, e);
                throw new RuntimeException("JDBC Async Lookup Failed", e);
            }
        }, executorService);
    }
}

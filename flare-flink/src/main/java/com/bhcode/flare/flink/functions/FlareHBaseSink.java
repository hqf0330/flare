package com.bhcode.flare.flink.functions;

import com.bhcode.flare.connector.hbase.HBaseConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 * Base class for HBase sink with buffered mutation support.
 */
@Slf4j
public abstract class FlareHBaseSink<IN> extends RichSinkFunction<IN> {

    protected final int keyNum;
    protected transient Connection connection;
    protected transient BufferedMutator mutator;

    public FlareHBaseSink() {
        this(1);
    }

    public FlareHBaseSink(int keyNum) {
        this.keyNum = keyNum;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.connection = HBaseConnector.getConnection(keyNum);
        TableName tableName = HBaseConnector.getTableName(keyNum);
        if (tableName == null) {
            throw new IllegalStateException("HBase table name is required for sink, keyNum=" + keyNum);
        }
        this.mutator = this.connection.getBufferedMutator(tableName);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (mutator != null) mutator.close();
        // Connection is managed by HBaseConnector, don't close it here if shared
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        Mutation mutation = convert(value);
        if (mutation != null) {
            mutator.mutate(mutation);
        }
    }

    /**
     * Convert input record to HBase Mutation (Put or Delete).
     */
    protected abstract Mutation convert(IN value) throws Exception;
}

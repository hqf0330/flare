/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bhcode.flare.connector;

import com.bhcode.flare.common.util.PropUtils;
import com.bhcode.flare.connector.jdbc.JdbcConnector;
import com.bhcode.flare.connector.kafka.KafkaConnector;
import com.bhcode.flare.connector.hbase.HBaseConnector;
import com.bhcode.flare.core.anno.connector.Jdbc;
import com.bhcode.flare.core.anno.connector.Kafka;
import com.bhcode.flare.core.anno.connector.HBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.util.function.BiConsumer;

/**
 * Flink connectors facade. Concrete connectors live in subpackages.
 */
public final class FlinkConnectors {

    private FlinkConnectors() {
        // Utility class
    }

    public static void applyConnectorAnnotations(Class<?> targetClass) {
        Kafka[] kafkaAnnos = targetClass.getAnnotationsByType(Kafka.class);
        if (kafkaAnnos != null) {
            for (Kafka kafka : kafkaAnnos) {
                String prefix = KafkaConnector.kafkaPrefix(kafka.keyNum());
                setIfNotBlank(prefix + "bootstrap.servers", kafka.brokers());
                setIfNotBlank(prefix + "topic", kafka.topics());
                setIfNotBlank(prefix + "group.id", kafka.groupId());
                setIfNotBlank(prefix + "starting.offsets", kafka.startingOffset());
                if (kafka.startFromTimestamp() > 0) {
                    PropUtils.setProperty(prefix + "startFromTimestamp", String.valueOf(kafka.startFromTimestamp()));
                }
                if (kafka.config() != null && kafka.config().length > 0) {
                    for (String conf : kafka.config()) {
                        if (conf != null && conf.contains("=")) {
                            String[] kv = conf.split("=", 2);
                            PropUtils.setProperty(prefix + "props." + kv[0].trim(), kv[1].trim());
                        }
                    }
                }
                setIfNotBlank(prefix + "watermark.strategy", kafka.watermarkStrategy());
                PropUtils.setProperty(prefix + "watermark.maxOutOfOrderness", String.valueOf(kafka.watermarkMaxOutOfOrderness()));
                setIfNotBlank(prefix + "watermark.timestampField", kafka.watermarkTimestampField());
            }
        }

        Jdbc[] jdbcAnnos = targetClass.getAnnotationsByType(Jdbc.class);
        if (jdbcAnnos != null) {
            for (Jdbc jdbc : jdbcAnnos) {
                String prefix = JdbcConnector.jdbcPrefix(jdbc.keyNum());
                setIfNotBlank(prefix + "url", jdbc.url());
                setIfNotBlank(prefix + "user", jdbc.username());
                setIfNotBlank(prefix + "password", jdbc.password());
                setIfNotBlank(prefix + "driver", jdbc.driver());
                setIfNotBlank(prefix + "sql", jdbc.sql());
                PropUtils.setProperty(prefix + "batch.size", String.valueOf(jdbc.batchSize()));
                PropUtils.setProperty(prefix + "batch.interval.ms", String.valueOf(jdbc.batchIntervalMs()));
                PropUtils.setProperty(prefix + "max.retries", String.valueOf(jdbc.maxRetries()));
                setIfNotBlank(prefix + "upsert.mode", jdbc.upsertMode());
                setIfNotBlank(prefix + "key.columns", jdbc.keyColumns());
            }
        }

        HBase[] hbaseAnnos = targetClass.getAnnotationsByType(HBase.class);
        if (hbaseAnnos != null) {
            for (HBase hbase : hbaseAnnos) {
                String prefix = HBaseConnector.hbasePrefix(hbase.keyNum());
                setIfNotBlank(prefix + "zk.quorum", hbase.zkQuorum());
                setIfNotBlank(prefix + "zk.port", hbase.zkPort());
                setIfNotBlank(prefix + "znode.parent", hbase.znodeParent());
                setIfNotBlank(prefix + "table.name", hbase.tableName());
            }
        }
    }

    public static <T> DataStream<T> kafkaSourceFromConf(
            StreamExecutionEnvironment env,
            Class<T> clazz,
            String topicOverride,
            int keyNum
    ) {
        return KafkaConnector.kafkaSourceFromConf(env, clazz, topicOverride, keyNum);
    }

    public static DataStream<String> kafkaSourceFromConf(
            StreamExecutionEnvironment env,
            String topicOverride,
            int keyNum
    ) {
        return KafkaConnector.kafkaSourceFromConf(env, topicOverride, keyNum);
    }

    public static <T> void jdbcSinkFromConf(
            DataStream<T> stream,
            BiConsumer<PreparedStatement, T> binder,
            int keyNum
    ) {
        JdbcConnector.jdbcSinkFromConf(stream, binder, keyNum);
    }

    private static void setIfNotBlank(String key, String value) {
        if (key == null || key.trim().isEmpty()) {
            return;
        }
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        PropUtils.setProperty(key, value.trim());
    }
}

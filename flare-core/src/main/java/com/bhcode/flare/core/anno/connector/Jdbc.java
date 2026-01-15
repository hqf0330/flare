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

package com.bhcode.flare.core.anno.connector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * JDBC connector annotation (Flink)
 * <p>
 * Supports multi-config via keyNum.
 * </p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Jdbcs.class)
public @interface Jdbc {

    /**
     * Sequence number for multi JDBC configs.
     */
    int keyNum() default 1;

    String url();

    String username() default "";

    String password() default "";

    String driver() default "";

    /**
     * SQL template used by sink (optional, can be provided via properties).
     */
    String sql() default "";

    /**
     * Batch size for JDBC sink.
     */
    int batchSize() default 500;

    /**
     * Batch interval in milliseconds for JDBC sink.
     */
    long batchIntervalMs() default 0;

    /**
     * Max retries for JDBC sink.
     */
    int maxRetries() default 3;

    /**
     * Upsert mode: none / mysql / postgresql.
     */
    String upsertMode() default "none";

    /**
     * Key columns for upsert (comma separated).
     */
    String keyColumns() default "";
}

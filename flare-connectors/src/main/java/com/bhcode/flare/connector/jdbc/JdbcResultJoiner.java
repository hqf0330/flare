package com.bhcode.flare.connector.jdbc;

import java.io.Serializable;

/**
 * Functional interface for joining lookup results with main stream records.
 */
@FunctionalInterface
public interface JdbcResultJoiner<DIM, IN, OUT> extends Serializable {
    OUT join(DIM dim, IN input) throws Exception;
}

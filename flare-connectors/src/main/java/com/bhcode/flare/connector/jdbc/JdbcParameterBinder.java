package com.bhcode.flare.connector.jdbc;

import java.sql.PreparedStatement;
import java.io.Serializable;

/**
 * Functional interface for binding parameters to a PreparedStatement.
 */
@FunctionalInterface
public interface JdbcParameterBinder<IN> extends Serializable {
    void accept(PreparedStatement ps, IN input) throws Exception;
}

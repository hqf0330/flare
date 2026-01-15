package com.bhcode.flare.connector.jdbc;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcUpsertUtils {

    /**
     * 为 MySQL 生成 ON DUPLICATE KEY UPDATE 语句
     * 
     * @param sql        原始 INSERT 语句，如 "INSERT INTO table(id, name, age) VALUES (?, ?, ?)"
     * @param keyColumns 主键列，如 "id"
     * @return 增强后的 SQL
     */
    public static String buildMysqlUpsertSql(String sql, String keyColumns) {
        if (sql == null || !sql.toUpperCase().contains("INSERT INTO")) {
            return sql;
        }

        // 简单的解析逻辑：提取列名部分
        try {
            int startBracket = sql.indexOf("(");
            int endBracket = sql.indexOf(")", startBracket);
            if (startBracket == -1 || endBracket == -1) return sql;

            String columnsPart = sql.substring(startBracket + 1, endBracket);
            String[] columns = columnsPart.split(",");
            
            StringBuilder sb = new StringBuilder(sql);
            sb.append(" ON DUPLICATE KEY UPDATE ");
            
            boolean first = true;
            for (String col : columns) {
                String trimmedCol = col.trim();
                // 跳过主键列
                if (keyColumns != null && keyColumns.contains(trimmedCol)) {
                    continue;
                }
                if (!first) sb.append(", ");
                sb.append(trimmedCol).append("=VALUES(").append(trimmedCol).append(")");
                first = false;
            }
            return sb.toString();
        } catch (Exception e) {
            log.warn("Failed to auto-generate MySQL upsert SQL, using original SQL: {}", sql);
            return sql;
        }
    }
}

package com.jd.jr.daat.dw.lineage.domains.lineage.column;

/**
 * 字段级别血缘字段节点类型
 */
public enum ColumnLineageColumnNodeType {
    /**
     * Hive 表字段
     */
    HIVE_TABLE_COLUMN("HIVE_TABLE_COLUMN"),

    /**
     * Hive 表字段 (通过 * 查询得到)
     */
    HIVE_TABLE_COLUMN_BY_ALL("HIVE_TABLE_COLUMN_BY_ALL"),

    /**
     * Hive 临时表字段
     */
    HIVE_TMP_TABLE_COLUMN("HIVE_TMP_TABLE_COLUMN"),

    /**
     * Hive 子查询表字段
     */
    HIVE_SUB_QUERY_TABLE_COLUMN("HIVE_SUB_QUERY_TABLE_COLUMN"),

    /**
     * Hive 子查询表字段 (通过 * 查询得到)
     */
    HIVE_SUB_QUERY_TABLE_COLUMN_BY_ALL("HIVE_SUB_QUERY_TABLE_COLUMN_BY_ALL"),

    /**
     * 未知的
     */
    UNKNOWN("UNKNOWN"),

    /**
     * 由于不支持的 Relation 节点导致的停止节点
     */
    STOP("STOP");

    ColumnLineageColumnNodeType(String name) {
        this.name = name;
    }

    /**
     * 名称
     */
    public String name;
}

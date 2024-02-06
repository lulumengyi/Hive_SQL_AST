package com.jd.jr.daat.dw.lineage.domains.basic;

public enum ColumnType {
    /**
     * Hive 字段
     */
    HIVE_COLUMN("HIVE_COLUMN", 1002),

    /**
     * Hive 分区字段
     */
    HIVE_PARTITION_COLUMN("HIVE_PARTITION_COLUMN", 1007),

    /**
     * MySQL 字段
     */
    MYSQL_COLUMN("MYSQL_COLUMN", 2002);

    ColumnType(String name, int code) {
        this.name = name;
        this.code = code;
    }

    /**
     * 名称
     */
    public String name;

    /**
     * 代码
     */
    public int code;
}

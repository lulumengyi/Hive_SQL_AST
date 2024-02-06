package com.jd.jr.daat.dw.lineage.domains.basic;

public enum TableType {
    /**
     * Hive 表
     */
    HIVE_TABLE("HIVE_TABLE", 1001),

    /**
     * Hive 视图
     */
    HIVE_VIEW("HIVE_VIEW", 1006),

    /**
     * MySQL 表
     */
    MYSQL_TABLE("MYSQL_TABLE", 2001);

    TableType(String name, int code) {
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

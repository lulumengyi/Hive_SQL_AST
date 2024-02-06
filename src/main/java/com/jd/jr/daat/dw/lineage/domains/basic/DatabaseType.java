package com.jd.jr.daat.dw.lineage.domains.basic;

public enum DatabaseType {
    /**
     * Hive 数据库
     */
    HIVE_DB("HIVE_DB"),

    /**
     * MySQL 数据库
     */
    MYSQL_DB("MYSQL_DB");

    DatabaseType(String name) {
        this.name = name;
    }

    /**
     * 名称
     */
    public String name;
}

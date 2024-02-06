package com.jd.jr.daat.dw.lineage.domains.basic;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Database {
    /**
     * 数据库名称
     */
    private String name;

    /**
     * 数据库类型
     */
    private DatabaseType type;

    /**
     * Hive 临时表库
     */
    public static final String[] HIVE_TMP_DATABASES = {"TMP"};
}

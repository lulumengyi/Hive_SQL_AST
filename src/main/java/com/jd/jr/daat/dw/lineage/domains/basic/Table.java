package com.jd.jr.daat.dw.lineage.domains.basic;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Table {
    /**
     * 表名称
     */
    private String name;

    /**
     * 表类型
     */
    private TableType type;

    /**
     * 对应的数据库
     */
    private Database database;
}

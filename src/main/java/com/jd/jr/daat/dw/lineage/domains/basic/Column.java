package com.jd.jr.daat.dw.lineage.domains.basic;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Column {
    /**
     * 字段名称
     */
    private String name;

    /**
     * 字段类型
     */
    private ColumnType type;

    /**
     * 对应的表
     */
    private Table table;
}

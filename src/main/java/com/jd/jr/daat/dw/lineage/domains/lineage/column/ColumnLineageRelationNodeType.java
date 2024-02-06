package com.jd.jr.daat.dw.lineage.domains.lineage.column;

/**
 * 字段级别血缘关系节点类型
 */
public enum ColumnLineageRelationNodeType {
    /**
     * 直接关系
     *
     * 例如：CREATE TABLE table2 AS SELECT col FROM table1 (table2.col -> table1.col)
     */
    DIRECT("DIRECT"),

    /**
     * 函数关系
     *
     * 例如：CREATE TABLE table2 AS SELECT TRIM(col) AS col_trim FROM table1; (table2.col_trim -> table1.col)
     */
    FUNCTION("FUNCTION"),

    /**
     * 别名关系
     *
     * 例如 :  select SUM(user_actual_pay_amount) ord_amount
     *
     */
    ALIAS("ALIAS"),

    CASE("CASE"),

    CAST("CAST"),

    /**
     * 不支持的关系
     */
    UNSUPPORTED("UNSUPPORTED");

    ColumnLineageRelationNodeType(String name) {
        this.name = name;
    }

    /**
     * 名称
     */
    public String name;
}

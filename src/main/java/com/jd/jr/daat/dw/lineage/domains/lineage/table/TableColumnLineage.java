package com.jd.jr.daat.dw.lineage.domains.lineage.table;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jd.jr.daat.dw.lineage.domains.lineage.column.ColumnLineageColumnNode;
import com.jd.jr.daat.dw.lineage.domains.basic.Column;
import com.jd.jr.daat.dw.lineage.domains.basic.Table;
import lombok.Data;

import java.util.Map;
import java.util.Set;

/**
 * 表的字段级别血缘
 */
@Data
public class TableColumnLineage {
    /**
     * 表
     */
    private Table table;

    /**
     * 字段
     */
    private Set<Column> columns;

    /**
     * 字段血缘
     */
    private Map<Column, ColumnLineageColumnNode> columnLineages;

    public TableColumnLineage() {
        this.columns = Sets.newHashSet();
        this.columnLineages = Maps.newHashMap();
    }

    /**
     * Description:
     *   添加字段
     *
     * @param column 字段
     */
    public void addColumn(Column column) {
        this.columns.add(column);
    }

    /**
     * Description:
     *   添加字段级别血缘字段根节点
     *
     * @param column 字段
     * @param columnNode 字段级别血缘字段根节点
     */
    public void addColumnLineageColumnRootNode(Column column, ColumnLineageColumnNode columnNode) {
        this.columnLineages.put(column, columnNode);
    }
}
